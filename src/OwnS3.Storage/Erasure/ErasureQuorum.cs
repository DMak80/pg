namespace OwnS3.Storage.Erasure;

// Кворумы канона 06 §7: read = k; write = k, при k == m — k+1 (иначе две
// «половины» сета могли бы закоммитить разные версии без пересечения).
public static class ErasureQuorum
{
    public static int Read(int k, int m) => k;

    public static int Write(int k, int m) => k == m ? k + 1 : k;
}
