﻿// Copyright (c) 2020 Bitcoin Association

namespace nChain.CreateDB
{
  public interface ICreateDB
  {
    bool DoCreateDB(string projectName, RDBMS rdbms, out string errorMessage, out string errorMessageShort);
    bool DatabaseExists(string projectName, RDBMS rdbms);
  }
}
