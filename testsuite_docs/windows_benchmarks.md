
perfkitbenchmarker.windows_benchmarks.fio_benchmark:
  --fio_file_size: "filesize" field of the global section of the fio config.
    This is the size of the individual files. Default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)
  --fio_random_read_parallel_size: "size" field of the random_read_parallel
    section of the fio config. This is the size of I/O for this job. fio will
    run until this many bytes have been transferred. The default is 4 * (System
    Memory) or 100GB, whichever is smaller.
    (an integer)
  --fio_random_read_size: "size" field of the random_read section of the fio
    config. This is the size of I/O for this job. fio will run until this many
    bytes have been transferred. The default is 4 * (System Memory) or 100GB,
    whichever is smaller.
    (an integer)
  --fio_random_write_size: "size" field of the random_write section of the fio
    config. This is the size of I/O for this job. fio will run until this many
    bytes have been transferred. The default is 4 * (System Memory) or 100GB,
    whichever is smaller.
    (an integer)
  --fio_sequential_read_size: "size" field of the sequential_read section of the
    fio config. This is the size of I/O for this job. fio will run until this
    many bytes have been transferred. The default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)
  --fio_sequential_write_size: "size" field of the sequential_write section of
    the fio config. This is the size of I/O for this job. fio will run until
    this many bytes have been transferred. The default is 4 * (System Memory) or
    100GB, whichever is smaller.
    (an integer)
