# Script to Send Predefined Queries to ArangoDB

- Author: Volker Hoffmann <volker.hoffmann@sintef.no>

## Prerequisites

0. Linux/Mac
1. Install Python (Anaconda/Miniconda, Enthought, or System)
2. Install PyArango (`pip install pyarango`)

## Usage

0. Install dependencies
1. Edit your query as appropriate (`query.aql`)
2. Open `query.py`, find the `connect_to_database()` function, make sure you're connecting to the correct database and port.
3. Make sure you have a list of keywords IDs you want to query for. For example, you could have file `keywords.json` looking like:

```
[
  { "key": "3317658057" },
  { "key": "12219921" } 
]
```

4. Call the script `python ./query.py --keyfile keywords.json`
5. If you know your queries take a while (or you run a lot of queries), edit `query.sh` as appropriate, call it in a screen session, and go home for the night.
6. For each keyword ID (for example, 123) you query, the script will write a file `result_123.json`.

## References/Links

- [Download Anaconda](https://www.anaconda.com/)
- [Download Miniconda](https://conda.io/miniconda.html)
- [Down Enthough Python](https://www.enthought.com/product/enthought-python-distribution/)
- [PyArango Code](https://github.com/tariqdaouda/pyArango)
- [PyArango Doc](http://bioinfo.iric.ca/~daoudat/pyArango/)
