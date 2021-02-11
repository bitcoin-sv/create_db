using System;

namespace nChain.CreateDB.DB
{
  public class DBFactory
  {
    public static IDB GetDB(RDBMS rdbms)
    {
      if (rdbms == RDBMS.Postgres)
      {
        return new DB.Postgres.DBPostgres();
      }
      else
      {
        throw new Exception($"{rdbms} not supported");
      }
    }
  }
}
