﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace nChain.CreateDB.Tools
{
  public class ScriptPathTools
  {
    private const string DB_FOLDER_SUFFIX = "Database";
    private const string DB_SCRIPTS_FOLDER_NAME = "Scripts";

    private const string SCRIPT_FILE_SYSTEM_MARKER = "SYS";
    private const string SCRIPT_FILE_MASTER_MARKER = "MASTER";

    /// <summary>
    /// Method tries to find scritps folder for given project starting from current executing assembly location.
    /// </summary>
    /// <param name="projectName">Name of the project</param>
    /// <param name="rdbms">Type of the database</param>
    /// <returns>Root script folder that ends with projectName.Database\Scripts\rdbms</returns>
    public static string FindScripts(string projectName, DB.RDBMS rdbms)
    {
      string path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
      string dbFolderName = String.Join('.', projectName, DB_FOLDER_SUFFIX);

      // Try to locate folder with name "projectName.Database" 
      // Start at executing assembly location and walk back up to 6 levels while checking for existance of database folder at every level.
      for (int i = 0; i < 6; i++)
      {
        string testData = Path.Combine(path, dbFolderName);
        if (Directory.Exists(testData))
        {
          // Generate scripts root path that ends with "MyProject.Database\Scripts\Postgres"
          string scriptsRoot = Path.Combine(testData, DB_SCRIPTS_FOLDER_NAME, rdbms.ToString());
          // Verify that root exists
          if (!Directory.Exists(scriptsRoot))
            throw new ApplicationException($"Folder { scriptsRoot } with custom scripts does not exist.");
          return scriptsRoot;
        }
        path = Path.Combine(path, "..");
      }
      throw new ApplicationException($"Can not find folder '{dbFolderName}' near location {Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)}");
    }

    public static bool IsUsingMasterConnectionString(string filePath)
    {
      return IsMarkerPresent(filePath, SCRIPT_FILE_MASTER_MARKER);
    }

    public static bool IsUsingSystemConnectionString(string filePath)
    {
      return IsMarkerPresent(filePath, SCRIPT_FILE_SYSTEM_MARKER);
    }

    private static bool IsMarkerPresent(string filePath, string marker)
    {
      // 0101_MARKER_ScriptName.sql
      string fileName = Path.GetFileName(filePath);
      int nameStart = fileName.IndexOf("_") + 1;
      if (nameStart < 0 || (fileName.StartsWith("_"))) // we ignore scripts that start with _
      {
        return false;
      }
      int nameLength = fileName.Substring(nameStart).IndexOf("_");
      if (nameLength < 0)
      {
        // if not specified use default connection string
        return false;
      }
      return fileName.Substring(nameStart, nameLength) == marker;
    }
  }
}
