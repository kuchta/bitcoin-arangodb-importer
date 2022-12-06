**Import bitcoin database into ArangoDB**

*Unless you specify --clean option it continues where it left off last time overwriting entries from last block*

```
  Usage: index [options]
  
  Options:

    -V, --version          output the version number
    -v, --verbose          Increase verbosity
    -d, --debug            Increase verbosity of debug messages
    -a, --async            Process transactions asynchronously
    -r, --retries <n>      Number of retries in case of conflict
    -w, --max-workers <n>  Maximal number of workers
    -o, --dont-overwrite   Don't overwrite existing entries
    -p, --perf             Increase performace report verbosity
    -c, --clean            Clean database before import
    -h, --help             output usage information
```

Notes: 
* Import take days and the current schema take three times more space that the bitcoin full-node database. 
Given the size of actual bitcoin database 1