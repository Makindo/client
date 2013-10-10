# HaystaqDNA / Makindo API Client

The purpose of this script is to match Makindo Person objects with
InfoUSA records. Each matching process has three steps:

1. Retrieve a Person object from the Makindo API.
2. Match the Person object to InfoUSA records based on personal details
   such as name and location.
3. Report to the Makindo API whether the Person object exactly matches,
   ambiguously matches, or does not match any InfoUSA records. Also
   include basic demographic data from InfoUSA.

## Dependencies

This script was developed using Python 2.7.5 and the following libraries:

* [MySQL-python 1.2.4](https://pypi.python.org/pypi/MySQL-python/1.2.4)
* [requests 2.0.0](https://pypi.python.org/pypi/requests/1.2.3)

Note also the external file `Parameters.json`, which includes database
and API connection details. This file resembles the following structure:

    {
        "mysql": {
            "host": "$DB_HOST",
            "user": "$DB_USER",
            "passwd": "$DB_PASSWD",
            "db": "$DB_NAME"
        },
        "makindo": {
            "token": "$API_KEY"
        }
    }

## Related Documentation

This client uses the Makindo API, so it is necessary to comply with its
[documentation](https://github.com/Makindo/api).

## TODO

* More sophisticated matching algorithms are necessary. For example, in
  cases of ambiguous query results a subsequent, broader search should
  be conducted. See [Haystaq_pycli](https://github.com/Makindo/Haystaq_pycli)
  for an example.
* It may be desirable to modify the Makindo API to accept two
  additional parameters. The first would be the number of ambiguous
  matches. Obviously, matches with two ambiguous Persons would be easier
  to de-duplicate than matches with hundreds. The second would be the
  version of the matching script. This would allow Makindo to re-match
  some Persons after the matching script is updated.

## Contributors

Ross Petchler (<ross@haystaqdna.com>) originally authored this script.

John Masterson (<john@makindo.io>) provided technical advice on the
behalf of Makindo.

Rachel Shorey (<rachel@haystaqdna.com>) and Brad Wieneke
(<brad@haystaqdna.com>) provided helpful initial code.


