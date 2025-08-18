// LogTapestry.Core/NtfsUtils.cs
using System.Runtime.InteropServices;

namespace LogTapestry.Core
{
  public static partial class NtfsUtils
  {
    public record FileIdentifier(long VolumeSerial, ulong FileId);

    public static FileIdentifier? GetFileIdentifier(string filePath)
    {
      using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
      var handle = fs.SafeFileHandle.DangerousGetHandle();

      BY_HANDLE_FILE_INFORMATION info;
      if (!GetFileInformationByHandle(handle, out info))
        return null;

      long volumeSerial = info.dwVolumeSerialNumber;
      ulong fileId = ((ulong)info.nFileIndexHigh << 32) | info.nFileIndexLow;
      return new FileIdentifier(volumeSerial, fileId);
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct BY_HANDLE_FILE_INFORMATION
    {
      public uint dwFileAttributes;
      public System.Runtime.InteropServices.ComTypes.FILETIME ftCreationTime;
      public System.Runtime.InteropServices.ComTypes.FILETIME ftLastAccessTime;
      public System.Runtime.InteropServices.ComTypes.FILETIME ftLastWriteTime;
      public uint dwVolumeSerialNumber;
      public uint nFileSizeHigh;
      public uint nFileSizeLow;
      public uint nNumberOfLinks;
      public uint nFileIndexHigh;
      public uint nFileIndexLow;
    }

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetFileInformationByHandle(IntPtr hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);
  }
}
