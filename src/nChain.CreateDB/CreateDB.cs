// Copyright (c) 2020 Bitcoin Association

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using nChain.CreateDB.Tools;
using nChain.CreateDB.DB;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace nChain.CreateDB
{
  public class CreateDB
  {
    readonly string projectName;
    readonly RDBMS rdbms;
    readonly IDB db;
    
    readonly ILogger<CreateDB> logger;

    readonly string scriptsRoot;
    readonly string connectionStringDDL; // connection string to database with user that has DDL rights
    readonly string connectionStringMaster; // connection string to database wiht superuser
    readonly string connectionStringSystem; // connection string to system database wiht superuser

    public CreateDB(ILogger<CreateDB> logger, string projectName, RDBMS rdbms, string connectionStringDDL, string connectionStringMaster = null, string scriptsRoot = null)
    {
      this.projectName = projectName;
      this.rdbms = rdbms;

      this.connectionStringDDL = connectionStringDDL;
      this.connectionStringMaster = connectionStringMaster;
      this.scriptsRoot = scriptsRoot; 

      this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

      db = DBFactory.GetDB(rdbms);
      if (connectionStringMaster != null)
        connectionStringSystem = db.GetConnectionStringWithDefaultDatabaseName(connectionStringMaster);

    }

    public bool CreateDatabase(out string errorMessage, out string errorMessageShort)
    {
      errorMessage = "";
      errorMessageShort = "";

      try
      {
        string rootFolder = scriptsRoot ?? ScriptPathTools.FindScripts(projectName, rdbms);
        DBFolders dbFolders = new DBFolders(projectName, rootFolder, logger);

        string errorMessageLocal = "";
        if (!string.IsNullOrEmpty(connectionStringMaster) && !string.IsNullOrEmpty(dbFolders.CreateDBFolderToProcess))
        {
          // we first process CreateDBFoldersToProcess - but only if no database exists yet
          // we use connectionStringMaster and connectionStringSystem for these scripts, because we need stronger permissions to create database
          if (!ProcessDBFolder(dbFolders.CreateDBFolderToProcess, out errorMessageLocal, out errorMessageShort))
          {
            errorMessage = $"Executing scripts from createDB folder '{ dbFolders.CreateDBFolderToProcess }' returned error: '{ errorMessageLocal }'.";
            if (dbFolders.ScriptFoldersToProcess.Count > 0)
            {
              errorMessage += Environment.NewLine + "Following folders must still be processed: ";
              errorMessage += string.Join(Environment.NewLine, dbFolders.ScriptFoldersToProcess.ToArray());
            }
            return false;
          }          
        }
        
        // after folders with createDB naming, process the other (version)folders - ScriptFoldersToProcess
        for (int i = 0; i < dbFolders.ScriptFoldersToProcess.Count; i++)
        { 
          if (!ProcessScriptFolder(dbFolders.ScriptFoldersToProcess[i], out errorMessageLocal, out errorMessageShort))
          {
            errorMessage = $"Executing scripts from folder '{ dbFolders.ScriptFoldersToProcess[i] }' returned error: '{ errorMessageLocal }'.";
            if (i < dbFolders.ScriptFoldersToProcess.Count)
            {
              errorMessage += Environment.NewLine + "Following folders must still be processed: ";
              errorMessage += string.Join(Environment.NewLine, dbFolders.ScriptFoldersToProcess.Skip(i).ToArray());
            }
            return false;
          }
        }
      }
      catch (Exception e)
      {
        errorMessage = e.Message;
        errorMessage += Environment.NewLine + "StackTrace:" + Environment.NewLine + e.StackTrace;
        errorMessageShort = e.Message;
        return false;
      }
      return true;
    }

    public bool DatabaseExists()
    {
      string databaseName = db.GetDatabaseName(connectionStringDDL);
      logger.LogInformation($"Trying to connect to DB: '{databaseName}'");
      bool success = false;

      try
      {
        success = db.DatabaseExists(connectionStringDDL, databaseName);
      }
      catch (Exception ex)
      {
        logger.LogInformation($"Failed to connect to DB: '{databaseName}' with DDL connection.");
        if (string.IsNullOrEmpty(connectionStringSystem))
          throw ex;
      }
      // If connection fails retry with system connection string if present because
      // database could exist but DDL user does not have right privileges yet.
      if (!success && !string.IsNullOrEmpty(connectionStringSystem))
      {
        success = db.DatabaseExists(connectionStringSystem, databaseName);
      }
      return success;
    }

    private bool ProcessDBFolder(string dbFolder, out string errorMessage, out string errorMessageShort)
    {
      errorMessage = "";
      errorMessageShort = "";

      try
      {
        // folder example: Scripts\Postgres\00_CreateDB\
        // if database does not exist yet, process createDb scripts
        bool databaseExists = DatabaseExists();

        if (!databaseExists)
        {
          if (!ExecuteScripts(dbFolder, true, out errorMessage, out errorMessageShort))
          {
            return false;
          }
        }

        db.GetCurrentVersion(projectName, connectionStringDDL, out int currentVersion, out bool _);
        if (currentVersion == -1)
        {
          // create Version table and set current version to '0'
          CreateVersionTable(projectName, connectionStringDDL);

          if (databaseExists)
          {
            logger.LogWarning("Table version added. If you want to execute scripts in 00_CreateDB, drop database first.");
          }
        }        
      }
      catch (Exception e)
      {
        errorMessage = e.Message;
        errorMessage += Environment.NewLine + "StackTrace:" + Environment.NewLine + e.StackTrace;
        errorMessageShort = e.Message;
        return false;
      }
      return true;
    }

    private bool ProcessScriptFolder(string scriptFolder, out string errorMessage, out string errorMessageShort)
    {
      errorMessage = "";
      errorMessageShort = "";

      try
      {
        // folder example: Scripts\Postgres\01\
        int[] installedVersions;

        db.GetCurrentVersion(projectName, connectionStringDDL, out int currentVersion, out bool updating);
        installedVersions = new int[] { currentVersion };

        if (updating)
        {
          errorMessage = $"When updating database for project '{ projectName }' to new version { String.Join(",", installedVersions) } error was thrown." +
            "Before another update of database set the field UPDATING=0 in table VERSION with command:'UPDATE VERSION SET UPDATING = 0' or delete row to retry folder processing." +
            "(if you are using docker use command: docker exec -it [container name] psql -U merchant -a merchant_gateway -c 'UPDATE VERSION SET UPDATING = 0')";
          errorMessageShort = errorMessage;
          return false;
        }

        int newVersion = Int32.Parse(Path.GetFileName(scriptFolder));
        if (installedVersions.Any(x => x < newVersion))
        {
          db.StartUpdating(projectName, newVersion, connectionStringDDL);
          if (!ExecuteScripts(scriptFolder, false, out errorMessage, out errorMessageShort))
          {
            db.RemoveVersion(projectName, newVersion, connectionStringDDL);
            return false;
          }
          db.FinishUpdating(projectName, newVersion, connectionStringDDL);
        }        
      }
      catch (Exception e)
      {
        errorMessage = e.Message;
        errorMessage += Environment.NewLine + "StackTrace:" + Environment.NewLine + e.StackTrace;
        errorMessageShort = e.Message;
        return false;
      }
      return true;
    }

    private void CreateVersionTable(string projectName, string connectionString)
    {
      db.CreateVersionTable(connectionString);
      // after create we insert row with version 0
      db.StartUpdating(projectName, 0, connectionString);
      db.FinishUpdating(projectName, 0, connectionString);
    }

    private bool ExecuteScripts(string scriptsFolder, bool isCreateDBScript, out string errorMessage, out string errorMessageShort)
    {
      errorMessage = "";
      errorMessageShort = "";

      logger.LogInformation($"Execution of scripts from folder '{ scriptsFolder }'.");

      try
      {       
        string errorMessageScript = "";
        string[] files = GetScripts(scriptsFolder);

        // execute all scripts ...
        int i;
        for (i = 0; i < files.Length; i++)
        {
          if (!ExecuteScript(files[i], isCreateDBScript, out errorMessageScript))
          {
            errorMessage = $"Executing script '{ files[i] }' returned error: '{ errorMessageScript }'.";
            errorMessageShort = errorMessageScript;
            if (i < files.Length)
            {
              errorMessage += Environment.NewLine + "Following files must still be processed: ";
              while (i < files.Length)
              {
                errorMessage += Environment.NewLine + files[i];
                i++;
              }
            }
            return false;
          }
        }                
      }
      catch (Exception e)
      {
        errorMessage = e.Message;
        errorMessage += Environment.NewLine + "StackTrace:" + Environment.NewLine + e.StackTrace;
        errorMessageShort = e.Message;
        return false;
      }
      return true;
    }

    private bool ExecuteScript(string scriptFilename, bool isCreateDBScript, out string errorMessage)
    {
      errorMessage = "";
      try
      {
        if (isCreateDBScript)
        {
          ExecuteSqlScript(scriptFilename, ScriptPathTools.IsUsingSystemConnectionString(scriptFilename) ? connectionStringSystem : connectionStringMaster, true);
        }
        else
        {
          ExecuteSqlScript(scriptFilename, ScriptPathTools.IsUsingMasterConnectionString(scriptFilename) ? connectionStringSystem : connectionStringDDL);
        }
        return true;
      }
      catch (Exception e)
      {
        errorMessage = e.ToString();

        return false;
      }
    }

    private string[] GetScripts(string scriptsFolder)
    {
      List<string> files = new List<string>();

      if (Directory.Exists(scriptsFolder))
      {
        files.AddRange(GetSqlAndTxtScripts(scriptsFolder));

        files.Sort(0, files.Count, new ScriptNameSorter());
      }

      return files.ToArray(); 
    }

    private static string[] GetSqlAndTxtScripts(string scriptsFolder)
    {
      List<string> files = new List<string>();

      string[] filesDDL = Directory.GetFiles(scriptsFolder, "*.ddl");
      string[] filesSQL = Directory.GetFiles(scriptsFolder, "*.sql");
      string[] filesTXT = Directory.GetFiles(scriptsFolder, "*.txt");

      files.AddRange(filesDDL);
      files.AddRange(filesSQL);
      files.AddRange(filesTXT);

      files.Sort(0, files.Count, new ScriptNameSorter());

      return files.ToArray();
    }

    private void ExecuteSqlScript(string filePath, string connectionString, bool createDB = false)
    {
      logger.LogInformation($"Starting with execution of script: { filePath }.");

      int connectionTimeout = 30; 

      System.Text.Encoding encoding = DirectoryHelper.GetSqlScriptFileEncoding(filePath);
      db.ExecuteFileScript(connectionString, filePath, encoding, connectionTimeout, !createDB);

    }

   
  }
}
