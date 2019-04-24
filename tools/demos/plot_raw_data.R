library(ggplot2)
args <- commandArgs(trailingOnly = TRUE)
benchmark <- args[1]
xaxis_label <- args[2]
in_file <- paste(benchmark, 'data.csv', sep='_')
out_file <- paste(benchmark, 'cdf.png', sep='_')
df <- read.csv(in_file, col.names=c('breakdown', 'value'))
ggplot(data=df, aes(x=value, color=breakdown)) +
  stat_ecdf() +
  theme(legend.title=element_blank()) +
  xlab(xaxis_label) +
  ylab('Percentile') +
  expand_limits(x=0, y=0) +
  scale_y_continuous(labels = scales::percent)
ggsave(out_file)
