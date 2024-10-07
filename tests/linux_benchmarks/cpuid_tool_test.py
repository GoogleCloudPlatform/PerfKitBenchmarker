import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cpuid_tool_benchmark as cpuid_tool
from tests import pkb_common_test_case


class CpuidToolTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def testCpuIdToolOutputParser(self):
    partial_cpuid_output = """
    CPU:
      vendor_id = "GenuineIntel"
      version information (1/eax):
          processor type  = primary processor (0)
          family          = 0x6 (6)
          model           = 0xf (15)
          stepping id     = 0x0 (0)
          extended family = 0x0 (0)
          extended model  = 0x4 (4)
          (family synth)  = 0x6 (6)
          (model synth)   = 0x4f (79)
      miscellaneous (1/ebx):
          process local APIC physical ID = 0x0 (0)
          maximum IDs for CPUs in pkg    = 0x20 (32)
          CLFLUSH line size              = 0x8 (8)
          brand index                    = 0x0 (0)
      cache and TLB information (2):
          0x63: data TLB: 2M/4M pages, 4-way, 32 entries
                data TLB: 1G pages, 4-way, 4 entries
          0x03: data TLB: 4K pages, 4-way, 64 entries
          0x76: instruction TLB: 2M/4M pages, fully, 8 entries
          0xff: cache data is in CPUID leaf 4
          0xb5: instruction TLB: 4K, 8-way, 64 entries
          0xf0: 64 byte prefetching
          0xc3: L2 TLB: 4K/2M pages, 6-way, 1536 entries
      processor serial number = 0004-06F0-0000-0000-0000-0000
      Feature Extended Size (0x80000008/edx):
          max page count for INVLPGB instruction = 0x0 (0)
          RDPRU instruction max input support    = 0x0 (0)
      (instruction supported synth):
          MWAIT = false
      deterministic cache parameters (4):
          --- cache 0 ---
          cache type                         = data cache (1)
          cache level                        = 0x1 (1)
          self-initializing cache level      = true
          fully associative cache            = false
      (multi-processing synth) = multi-core (c=12), hyper-threaded (t=2)
      (multi-processing method) = Intel leaf 0xb
      (APIC widths synth): CORE_width=4 SMT_width=1
      (APIC synth): PKG_ID=0 CORE_ID=0 SMT_ID=0
      (uarch synth) = Intel Broadwell {shrink of Haswell}, 14nm
      (synth) = Intel Xeon E5-1600 / E5-2600 / E5-4600 v4 (Broadwell-E) / E7-4800 / E7-8800 v4 (Broadwell-EX) {shrink of Haswell}, 14nm
      """
    output = cpuid_tool.ParseCpuIdASCIIOutput(partial_cpuid_output)
    self.assertEqual(
        output.sections(),
        [
            'cpu',
            'cpu-version_information',
            'cpu-miscellaneous',
            'cpu-cache_and_tlb_information',
            'cpu-feature_extended_size',
            'cpu-instruction_supported_synth',
            'cpu-deterministic_cache_parameters',
            'cpu-deterministic_cache_parameters-cache_0',
        ],
    )
    self.assertEqual(
        list(dict(output['cpu-deterministic_cache_parameters-cache_0']).keys()),
        [
            'cache_type',
            'cache_level',
            'self-initializing_cache_level',
            'fully_associative_cache',
            'multi-processing_synth',
            'multi-processing_method',
            'apic_widths_synth_core_width',
            'apic_synth_pkg_id',
            'uarch_synth',
            'synth',
        ],
    )

  def testHexaDecimalParser(self):
    cpuid_output = """
    CPU:
      0x00000000 0x00: eax=0x0000000d ebx=0x68747541 ecx=0x444d4163 edx=0x69746e65
      0x00000001 0x00: eax=0x00830f10 ebx=0x0a180800 ecx=0xfef83203 edx=0x178bfbff
      0x00000002 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000
      0x00000003 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000
      0x00000004 0x00: eax=0x00000000 ebx=0x00000000 ecx=0x00000000 edx=0x00000000
    """
    data = cpuid_tool.ParseCpuIdHexadecimalOutput(cpuid_output)
    self.assertEqual(data['0x00000000_0x00_eax'], '0x0000000d')
    self.assertEqual(data['0x00000000_0x00_ebx'], '0x68747541')
    self.assertEqual(data['0x00000000_0x00_ecx'], '0x444d4163')
    self.assertEqual(data['0x00000000_0x00_edx'], '0x69746e65')
    self.assertEqual(data['0x00000004_0x00_edx'], '0x00000000')


if __name__ == '__main__':
  unittest.main()
