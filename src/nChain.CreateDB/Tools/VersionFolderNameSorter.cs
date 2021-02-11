using System;
using System.Collections.Generic;
using System.IO;

namespace nChain.CreateDB.Tools
{
  public class VersionFolderNameSorter : IComparer<string>
  {
    public int Compare(string folderName1, string folderName2)
    {
      try
      {
        folderName1 = new DirectoryInfo(folderName1).Name;
        folderName2 = new DirectoryInfo(folderName2).Name;

        int version1 = Int32.Parse(folderName1);
        int version2 = Int32.Parse(folderName2);

        return version1.CompareTo(version2);
      }
      catch (Exception)
      {
        return folderName1.CompareTo(folderName2);
      }
    }
  }
}
