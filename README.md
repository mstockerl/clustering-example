# clustering-example
Example project for the talk at the Search Meetup.

You can build the project using 

```
sbt assembly
```

## Index data

The code within the index package, let you index reviews of the yelp dataset. 
To fill the index:

```
java -cp target/scala-2.10/clustering-example.jar net.gutefrage.example.index.Indexer {clusterName} {pathToDataset}
```

## Cluster Data

```
java -cp target/scala-2.10/clustering-example.jar net.gutefrage.example.clustering.Clustering {clusterName}
```
