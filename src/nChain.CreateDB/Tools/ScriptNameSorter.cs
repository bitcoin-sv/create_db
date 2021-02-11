using System;
using System.Collections.Generic;
using System.IO;

namespace nChain.CreateDB.Tools
{
  public class ScriptNameSorter : IComparer<string>
  {
    public int Compare(string scriptName1, string scriptName2)
    {
      // First part of name (to first '_') is number of file inside version folder.

      try
      {
        scriptName1 = new FileInfo(scriptName1).Name;
        scriptName2 = new FileInfo(scriptName2).Name;

        string sPrefix1 = scriptName1.Substring(0, scriptName1.IndexOf("_"));
        int prefix1 = Int32.Parse(sPrefix1);

        string sPrefix2 = scriptName2.Substring(0, scriptName2.IndexOf("_"));
        int prefix2 = Int32.Parse(sPrefix2);

        return prefix1.CompareTo(prefix2);
      }
      catch (Exception)
      {
        return scriptName1.CompareTo(scriptName2);
      }
    }
  }
}
