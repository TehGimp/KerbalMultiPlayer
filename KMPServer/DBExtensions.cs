using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Text;

namespace KMPServer
{
    public static class DBExtensions
    {
        public static DbParameter AddWithValue(this DbParameterCollection cmd, string parameterName, object value)
        {
            var asSqlLite = cmd as SQLiteParameterCollection;

            if (asSqlLite != null)
            {
                return asSqlLite.AddWithValue(parameterName, value);
            }

            var asMySQL = cmd as MySqlParameterCollection;

            if (asMySQL != null)
            {
                return asMySQL.AddWithValue(parameterName, value);
            }

            throw new ArgumentException("Parameter Collection must be with SQLite or MySql");
        }
		
		public static void BackupDatabase(this DbConnection conn, DbConnection backupConn)
        {
            var asSqlLite = conn as SQLiteConnection;

            if (asSqlLite != null)
            {
                asSqlLite.BackupDatabase(backupConn as SQLiteConnection, "main", "main", -1, null, 0);
				return;
            }

            var asMySQL = conn as MySqlConnection;

            if (asMySQL != null)
            {
                //Do nothing, MySQL DB is entirely handled by MySQL server
				return;
            }

            throw new ArgumentException("Connection must be with SQLite or MySql");
        }
    }
}
