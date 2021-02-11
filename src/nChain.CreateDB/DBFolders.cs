// Copyright (c) 2020 Bitcoin Association

using Microsoft.Extensions.Logging;
using nChain.CreateDB.Tools;
using System;
using System.Collections.Generic;
using System.IO;

namespace nChain.CreateDB
{
  public class DBFolders
  {

    readonly string ProjectName;

    public string CreateDBFolderToProcess { get; private set; } = String.Empty; // folder with name "00_CreateDB"

    public List<string> ScriptFoldersToProcess { get; } = new List<string>(); // folders with version name

    private readonly HashSet<string> _projectAndVersions = new HashSet<string>();

    public DBFolders(string projectName, string pathToScripts, ILogger logger)
    {
      ProjectName = projectName;
      if (!Directory.Exists(pathToScripts))
        throw new ApplicationException($"Folder { pathToScripts } with scripts does not exist.");

      // Example: "[ApplicationName.Database]\Scripts\Postgres\": Postgres folder contains createDB or version folders with scripts.
      ProcessProjectDirectory(logger, pathToScripts);
    }

    public void WriteFolderNames(ILogger logger)
    {
      logger.LogInformation(" Folder for createDB:");
      if (!string.IsNullOrEmpty(CreateDBFolderToProcess))
      {
        logger.LogInformation(CreateDBFolderToProcess);
      }
      logger.LogInformation(" Folders with scripts:");
      foreach (string scriptFolder in ScriptFoldersToProcess)
      {
        logger.LogInformation(scriptFolder);
      }
    }

    private void ProcessProjectDirectory(ILogger logger, string pathToScripts)
    {
      foreach (string versionDirectoryName in DirectoryHelper.GetDirectories(pathToScripts))
      {
        string version = GetLastDirectoryName(versionDirectoryName);
        string projectAndVersion = (ProjectName + "#" + version).ToLower();
        if (version.ToLower() == "00_createdb")
        {
          if (!_projectAndVersions.Contains(projectAndVersion))
          {
            _projectAndVersions.Add(projectAndVersion);
            CreateDBFolderToProcess = versionDirectoryName;
          }
        }
        else if (IsVersionFolder(version))
        {
          if (!_projectAndVersions.Contains(projectAndVersion))
          {
            _projectAndVersions.Add(projectAndVersion);
            ScriptFoldersToProcess.Add(versionDirectoryName);
          }
        }
        else
        {
          // ignore this folder
          logger.LogInformation("WARNING!!!!");
          logger.LogInformation($"Folder '{ versionDirectoryName }' and its scripts will be ignored.");
        }
      }
    }

    private bool IsVersionFolder(string versionDirectoryName)
    {
      return Int32.TryParse(versionDirectoryName, out _);
    }

    private string GetLastDirectoryName(string directoryName)
    {
      return Path.GetFileName(directoryName);
    }

  }
}
