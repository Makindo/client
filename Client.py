"""Match Makindo Person objects with InfoUSA records.

Each matching process has three steps:

    1. Retrieve a Person object from the Makindo API.
    2. Match the Person object to InfoUSA records based on personal details
       such as name and location.
    3. Report to the Makindo API whether the Person object exactly matches,
       ambiguously matches, or does not match any InfoUSA records. Also
       include basic demographic data from InfoUSA.
"""


import codecs
import json

import pymysql
import requests


with open('parameters.json') as f:
    p = json.load(f)


conn = pymysql.connect(host=p['mysql']['host'],
                       user=p['mysql']['user'],
                       passwd=p['mysql']['passwd'],
                       database=p['mysql']['database'])
c = conn.cursor()


# Headers used for Makindo API calls.
# Complies with https://github.com/Makindo/api#request-headers.
headers = {'Authorization': 'Token token="{}"'.format(p['makindo']['token']),
           'Accept': 'application/json', 'Content-Type': 'application/json'}


# List of U.S. state abbreviations. Complies with ANSI standard INCITS 38:2009.
# Used to prevent malformed Makindo API responses from injecting SQL.
# Adapted from: https://en.wikipedia.org/wiki/List_of_U.S._state_abbreviations
states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
          'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
          'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
          'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
          'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']


def remove_missing(d):
    """Recursively remove items from a dictionary whose values are None."""
    for k, v in list(d.items()):
        if not v:
            del d[k]
    for v in d.values():
        if isinstance(v, dict):
            remove_missing(v)
    return d


def parse_names(person):
    """Parse the first name and last name of a Makindo Person.

    If the person has a defined first name and last name pair, use it.
    If the person has a missing or malformed first name and last name pair
    but has exactly one non-missing alternate first name and last name pair,
    use it instead.

    Frustratingly, the InfoUSA 2013 MySQL database uses utf8_swedish_ci
    collation instead of utf8_unicode_ci collation. Attempt to convert
    each first name and last name pair to latin-1; if either name contains
    a character not in the latin-1 character set, return Nones.

    Args:
        person: A dictionary corresponding to a Makindo Person object.

    Returns:
        A tuple of two strings corresponding to the person's first name and
            last name, or a tuple of two Nones if unable to parse names.
    """
    if person.get('name'):
        firstname, *middlenames, lastname = person['name'].split()
    elif person.get('names'):
        v = [i for i in person['names'] if all(i.values())]
        if len(v) != 1:
            return None, None
        firstname, lastname = v[0]['personal'], v[0]['family']
    else:
        return None, None

    try:
        return firstname.encode('latin-1'), lastname.encode('latin-1')
    except UnicodeEncodeError:
        return None, None


def parse_locations(person):
    """Parse the location of a Makindo Person.

    If the person has a defined state, use it.
    If the person has a missing state but has exactly one non-missing alternate
    state, use it instead.

    Args:
        person: A dictionary corresponding to a Makindo Person object.

    Returns:
        A string corresponding to the person's state, or None if unable to parse
            locations.
    """
    if person.get('location').get('state'):
        state = person['location']['state'].upper()
    elif person.get('locations'):
        v = {i['state'].upper() for i in person['locations'] if i['state']}
        if len(v) != 1:
            return
        state = v.pop()
    else:
        return

    if state not in states:
        return  # Prevent SQL injections via malformed state responses
    return state


def match(person):
    """Attempt to match a Makindo Person to an InfoUSA record.

    Args:
        person: A dictionary corresponding to a Makindo person record.

    Returns:
        A string indicating the status of the Person in the InfoUSA database.
    """
    firstname, lastname = parse_names(person)
    state = parse_locations(person)

    if not (firstname and lastname and state):
        return ('failed', None, None, None, None, None, None, None)

    query = """SELECT individualid,
                      CONCAT_WS(' ', firstname, lastname) AS name,
                      CASE WHEN gender = 'M' THEN 'male'
                           WHEN gender = 'F' THEN 'female'
                           ELSE NULL END AS gender,
                      age,
                      city,
                      state,
                      findincome
               FROM {}_indiv_raw
               WHERE firstname = %s
                 AND lastname = %s;""".format(state.lower())
    args = (firstname, lastname)

    try:
        num_results = c.execute(query, args)
    except:
        # Illegal mix of collations occurs because InfoUSA data use the
        # `(latin1_swedish_ci, IMPLICIT)` collation.
        return ('failed', None, None, None, None, None, None, None)

    _id, name, gender, age, city, state, findincome = None, None, None, None, None, None, None

    # Convert number of results into a status string.
    if num_results == 0:
        status = 'missing'
    elif num_results == 1:
        status = 'found'
        _id, name, gender, age, city, state, findincome = c.fetchone()
        age = int(age) if age else None
        findincome = int(findincome) if findincome else None
    elif num_results > 1:
        status = 'ambiguous'
    else:
        status = 'failed'

    return (status, _id, name, gender, age, city, state, findincome)


def patch(person_id, data):
    """Report to Makindo the status of a person in the InfoUSA database.

    See also:
        https://github.com/Makindo/api/blob/master/persons.md#patch-personsid

    Args:
        person_id: The Makindo person ID.
        status: The string status of the person in our databases.

    Returns:
        The status code of the PATCH request.
    """
    status, _id, name, gender, age, city, state, findincome = data

    if status not in {'found', 'ambiguous', 'missing', 'failed'}:
        raise ValueError("Invalid status: '{}'".format(status))

    data = {
                'person': {
                    'location': {
                        'city': city,
                        'state': state
                    },
                    'gender': gender,
                    'age': {
                        'maximum': age,
                        'minimum': age
                    },
                    'data': {
                        'income': findincome
                    },
                    'external_id': _id,
                    'name': name,
                    'status': status
                }
            }
    data = json.dumps(remove_missing(data))

    url = 'http://api.makindo.io/persons/{}'.format(person_id)
    r = requests.patch(url, data=data, headers=headers, verify=False)
    print(r.status_code, person_id, data)
    return r.status_code


def write_json(person):
    """Write a Person record as a JSON object line in a flat file."""
    with codecs.open('persons', 'a', 'utf8') as f:
        f.write(u'{}\n'.format(json.dumps(person, ensure_ascii=False)))


def main():

    url = 'http://api.makindo.io/persons'
    params = {'offset': 1, 'limit': 100, 'start': 330543}
    r = requests.get(url, params=params, headers=headers, verify=False)

    data = r.json()
    url = data['meta']['link']

    while url:
        r = requests.get(url, headers=headers, verify=False)

        if r.status_code != 200:
            print('Terminated with status code {}.'.format(r.status_code))
            break

        data = r.json()

        for person in data['persons']:
            write_json(person)
            resp = match(person)
            patch(person['id'], resp)

        url = data['meta']['next']

    print('Terminated with url\n{}.'.format(start))

    conn.close()


if __name__ == '__main__':
    main()


