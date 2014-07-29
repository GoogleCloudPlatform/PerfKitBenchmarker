#!/bin/bash

total_cpus=`nproc`

config_nvme()
{
  current_cpu=0
  for dev in /sys/bus/pci/drivers/nvme/*
  do
    if [ ! -d $dev ]
    then
      continue
    fi
    for irq_info in $dev/msi_irqs/*
    do
      if [ ! -f $irq_info ]
      then
        continue
      fi
      current_cpu=$((current_cpu % total_cpus))
      cpu_mask=`printf "%x" $((1<<current_cpu))`
      irq=$(basename $irq_info)$a
      echo Setting IRQ $irq smp_affinity to $cpu_mask
      echo $cpu_mask > /proc/irq/$irq/smp_affinity
      current_cpu=$((current_cpu+1))
    done
  done
}

config_scsi()
{
 irqs=()
 for device in /sys/bus/virtio/drivers/virtio_scsi/virtio*
 do
   ssd=0
   for target_path in $device/host*/target*/*
   do
     if [ ! -f $target_path/model ]
     then
       continue
     fi
     model=$(cat $target_path/model)
     if [[ $model =~ .*EphemeralDisk.* ]]
     then
       ssd=1
       for queue_path in $target_path/block/sd*/queue
       do
         echo noop > $queue_path/scheduler
         echo 0 > $queue_path/add_random
         echo 512 > $queue_path/nr_requests
         echo 0 > $queue_path/rotational
         echo 0 > $queue_path/rq_affinity
         echo 1 > $queue_path/nomerges
       done
     fi
   done
   if [[ $ssd == 1 ]]
   then
     request_queue=$(basename $device)-request
     irq=$(cat /proc/interrupts |grep $request_queue| awk '{print $1}'| sed 's/://')
     irqs+=($irq)
   fi
 done
 irq_count=${#irqs[@]}
 if [ $irq_count != 0 ]
 then
   stride=$((total_cpus / irq_count))
   stride=$((stride < 1 ? 1 : stride))
   current_cpu=0
   for irq in "${irqs[@]}"
   do
     current_cpu=$(($current_cpu % $total_cpus))
     cpu_mask=`printf "%x" $((1<<$current_cpu))`
     echo Setting IRQ $irq smp_affinity to $cpu_mask
     echo $cpu_mask > /proc/irq/$irq/smp_affinity
     current_cpu=$((current_cpu+stride))
   done
 fi
}

config_nvme
config_scsi
