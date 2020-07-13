#/bin/sh
# The script is hardcoded to look for interrupt queues with eth
# and ens in names.
service irqbalance stop
affinity_values=(00000001 00000002 00000004 00000008 00000010 00000020 00000040 00000080)
irqs=($(grep 'eth\|ens' /proc/interrupts|awk '{print $1}'|cut -d : -f 1))
irqLen=${#irqs[@]}
for (( i=0; i<${irqLen}; i++ ));
do
  echo $(printf "0000,00000000,00000000,00000000,${affinity_values[$i]}") > /proc/irq/${irqs[$i]}/smp_affinity;
  echo "IRQ ${irqs[$i]} =" $(cat /proc/irq/${irqs[$i]}/smp_affinity);
done
