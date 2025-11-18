import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A class designed (by gemini) to stress the Java JIT compiler, making the compilation phase take a
 * noticeable amount of time.
 *
 * <p>It works by creating many unique method implementations (via an interface) and calling them in
 * a hot loop.
 */
public class JitStressTest {

  // An interface to create polymorphic call sites.
  interface Operation {
    int execute(int input);
  }

  // Create dozens of unique implementations of the Operation interface.
  // Each one does a slightly different, non-trivial calculation to avoid being optimized away
  // easily.
  static class Operation1 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 31 + 17) ^ 0x5A5A;
    }
  }

  static class Operation2 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 29 - 13) ^ 0x5A5B;
    }
  }

  static class Operation3 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 37 + 19) ^ 0x5A5C;
    }
  }

  static class Operation4 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 41 - 23) ^ 0x5A5D;
    }
  }

  static class Operation5 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 43 + 29) ^ 0x5A5E;
    }
  }

  static class Operation6 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 47 - 31) ^ 0x5A5F;
    }
  }

  static class Operation7 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 53 + 37) ^ 0x5A6A;
    }
  }

  static class Operation8 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 59 - 41) ^ 0x5A6B;
    }
  }

  static class Operation9 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 61 + 43) ^ 0x5A6C;
    }
  }

  static class Operation10 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 67 - 47) ^ 0x5A6D;
    }
  }

  static class Operation11 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 71 + 53) ^ 0x5A6E;
    }
  }

  static class Operation12 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 73 - 59) ^ 0x5A6F;
    }
  }

  static class Operation13 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 79 + 61) ^ 0x5A7A;
    }
  }

  static class Operation14 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 83 - 67) ^ 0x5A7B;
    }
  }

  static class Operation15 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 89 + 71) ^ 0x5A7C;
    }
  }

  static class Operation16 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 97 - 73) ^ 0x5A7D;
    }
  }

  static class Operation17 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 101 + 79) ^ 0x5A7E;
    }
  }

  static class Operation18 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 103 - 83) ^ 0x5A7F;
    }
  }

  static class Operation19 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 107 + 89) ^ 0x5A8A;
    }
  }

  static class Operation20 implements Operation {
    @Override
    public int execute(int i) {
      return (i * 109 - 97) ^ 0x5A8B;
    }
  }

  public static void wasteTime() {
    System.out.println("Starting JIT stress test...");

    List<Operation> operations = new ArrayList<>();
    operations.add(new Operation1());
    operations.add(new Operation2());
    operations.add(new Operation3());
    operations.add(new Operation4());
    operations.add(new Operation5());
    operations.add(new Operation6());
    operations.add(new Operation7());
    operations.add(new Operation8());
    operations.add(new Operation9());
    operations.add(new Operation10());
    operations.add(new Operation11());
    operations.add(new Operation12());
    operations.add(new Operation13());
    operations.add(new Operation14());
    operations.add(new Operation15());
    operations.add(new Operation16());
    operations.add(new Operation17());
    operations.add(new Operation18());
    operations.add(new Operation19());
    operations.add(new Operation20());

    // Shuffling makes the call order less predictable
    Collections.shuffle(operations);

    // A very long-running loop to ensure the methods become "hot".
    final long iterations = 400_000_000L;
    long result = 0; // Use this to accumulate results.

    long startTime = System.nanoTime();
    System.out.println("Warm-up loop started. JIT compilation will occur now.");

    for (long i = 0; i < iterations; i++) {
      // Cycle through the list of operations. This creates a polymorphic call site
      // that is challenging for the JIT to optimize completely.
      Operation op = operations.get((int) (i % operations.size()));
      result += op.execute((int) i);
    }

    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

    System.out.println("\nTest finished.");
    System.out.printf("Execution time: %.2f seconds%n", durationSeconds);

    // IMPORTANT: Print the result to prevent the JIT from performing
    // dead code elimination and optimizing the entire loop away.
    System.out.println("Final result (to prevent dead code elimination): " + result);
  }
}
