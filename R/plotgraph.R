###
### Code to visualise the results 
###


### package.installer("igraph") might be needed if igraph is not installed already
library("igraph")

### Read out from pySpark
edges <- read.table("out.txt", stringsAsFactors=FALSE, sep = ';')
### Make a graph with membership from Power iteration clustering from Spark
g.all<- graph_from_data_frame(edges, directed = FALSE, vertices = unique(rbind(data.frame(vertex = edges[, 1], membership = edges[, 3]), data.frame(vertex = edges[, 2], membership = edges[, 4]))))

### Take connected component - due to some parse error we get disconnected stops 
g = decompose.graph(g.all)[[1]]

colbar <- rainbow(length(unique(edges[, 3])))


pdf("plots/PIC.pdf", width = 20, height = 20)
plot(g, vertex.size = 0.3, vertex.label.cex=0.3, edge.width=get.edge.attribute(g)$V5, vertex.color = colbar[edges[, 3]], layout = layout_nicely(g))
dev.off()


### Louvain clustering of graph
r.result <-cluster_louvain(g)
colbar <- rainbow(length(communities(r.result)))


pdf("plots/graph_louvain.pdf", width = 20, height = 20)
plot(g, vertex.size = 0.3, vertex.label.cex=0.3, edge.width=get.edge.attribute(g)$V5, vertex.color = colbar[membership(fgreedy)], layout = layout_with_gem(g))
dev.off()
