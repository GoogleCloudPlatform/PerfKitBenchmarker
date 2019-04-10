library(ggplot2)
args <- commandArgs(trailingOnly = TRUE)
benchmark <- args[1]
xaxis_label <- args[2]
in_file <- paste(benchmark, 'data.csv', sep='_')
out_file <- paste(benchmark, 'cdf.png', sep='_')
df <- read.csv(in_file, sep=',', col.names=c('label','latency','percentile'))
ggplot(data=df, aes(x=latency, y=percentile, group=label)) +
  geom_line(aes(color=label)) +
  theme(legend.title=element_blank()) +
  expand_limits(x=0, y=0) +
  xlab(xaxis_label) +
  ylab('Percentile') +
  xlim(0, 400)
ggsave(out_file)
