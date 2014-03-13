using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace KMPServer
{
    /// <summary>
    /// Database Helper taking the manual parts of queries away
    /// </summary>
    internal class DatabaseHelper
    {
        private const String SQLITE_INIT_SQL = "PRAGMA auto_vacuum = 1;PRAGMA synchronous = 0;";

        /// <summary>
        /// Connection String used for database connection.
        /// Set in Constructor. Read-Only
        /// </summary>
        internal String ConnectionString { get; private set; }

        /// <summary>
        /// Database Attributes for the connection
        /// </summary>
        internal DatabaseAttributes Attributes { get; private set; }

        /// <summary>
        /// Create a new Database Helper
        /// </summary>
        /// <param name="ConnectionString">Database Connection String</param>
        internal DatabaseHelper(String ConnectionString, DatabaseAttributes attributes)
        {
            this.ConnectionString = ConnectionString;
            this.Attributes = attributes;
        }

        /// <summary>
        /// Create default SQLite connection
        /// </summary>
        /// <param name="filePath">DB file path</param>
        /// <returns>DatabaseHelper for Database</returns>
        internal static DatabaseHelper CreateForSQLite(String filePath)
        {
            return new DatabaseHelper("Data Source=" + filePath, DatabaseAttributes.SQLite);
        }

        /// <summary>
        /// Create default MySQL connection
        /// </summary>
        /// <param name="connectionString">MySQL Connection String</param>
        /// <returns>DatabaseHelper for Database</returns>
        internal static DatabaseHelper CreateForMySQL(String connectionString)
        {
            return new DatabaseHelper(connectionString, DatabaseAttributes.MySQL | DatabaseAttributes.MyISAM);
        }

        /// <summary>
        /// Get connection for Database
        /// </summary>
        /// <returns>DbConnection of either MySqlConnection or SQLiteConnection</returns>
        private DbConnection Connection 
        {
            /* If we wanted to we could do our own sort of pooling to make it common between all engines */
            get
            {
                switch (Attributes & (DatabaseAttributes.SQLite | DatabaseAttributes.MySQL))
                {
                    case DatabaseAttributes.MySQL:
                        return new MySqlConnection(ConnectionString);
                    case DatabaseAttributes.SQLite:
                        return new SQLiteConnection(ConnectionString);
                }
                return null;
            }
        }

        /// <summary>
        /// Initate any state on the connection before running a command
        /// </summary>
        /// <param name="connection">Connection to initalize</param>
        private void InitConnection(DbConnection connection)
        {
            if (connection != null)
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                    if ((Attributes & DatabaseAttributes.SQLite) == DatabaseAttributes.SQLite)
                    {
                        // Init SQLite connection
                        _ExecuteNonQuery(connection, SQLITE_INIT_SQL);
                    }
                }
            }
        }

        /// <summary>
        /// Create a command object
        /// </summary>
        /// <param name="connection">Connection to create command object from</param>
        /// <param name="query">Query string to apply</param>
        /// <param name="parameters">Parameter as a string,value,string,value set</param>
        /// <returns>Returns a Text Command Type and with Query set</returns>
        private DbCommand CreateCommand(DbConnection connection, String query, params object[] parameters)
        {
            var cmdObj = connection.CreateCommand();
            cmdObj.CommandType = System.Data.CommandType.Text;
            cmdObj.CommandText = query;
            if (parameters != null && parameters.Length > 0 )
            {
                if (parameters.Length % 2 != 0) throw new IOException("Cannot create command with parameters. Argument count isn't a factor of 2");
                for (int i = 0; i < parameters.Length; i += 2)
                {
                    if (parameters[i] as String == null) throw new IOException(String.Format("Cannot convert {0} to Parameter key in CreateCommand", parameters[i] ?? "<null>"));
                    cmdObj.Parameters.AddWithValue(parameters[i] as String, parameters[i + 1]);
                }
            }
            return cmdObj;
        }

        /// <summary>
        /// Execute a NonQuery 
        /// </summary>
        /// <param name="connection">Connection to Execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Rows affected</returns>
        private int _ExecuteNonQuery(DbConnection connection, String query, params object[] parameters)
        {
            using (var command = CreateCommand(connection, query, parameters))
            {
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Execute a Scalar Query
        /// </summary>
        /// <param name="connection">Connection to execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Single result from Query</returns>
        private object _ExecuteScalar(DbConnection connection, String query, params object[] parameters)
        {
            using (var command = CreateCommand(connection, query, parameters))
            {
                return command.ExecuteScalar();
            }
        }

        /// <summary>
        /// Execute a Reader Query
        /// </summary>
        /// <param name="connection">Connection to invoke on</param>
        /// <param name="query">Query to execute</param>
        /// <param name="handler">Handler to invoke for each record</param>
        private void _ExecuteReader(DbConnection connection, String query, DbRecordHandler handler, params object[] parameters)
        {
            using (var command = CreateCommand(connection, query, parameters))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        try
                        {
                            handler(reader);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Error handling row in reader from DatabaseHelper: {0}", ex);
                        }
                    }                    
                }
            }
        }

        /// <summary>
        /// Execute a NonQuery 
        /// </summary>
        /// <param name="connection">Connection to Execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Rows affected</returns>
        internal int ExecuteNonQuery(String query, params object[] parameters)
        {
            using (var connection = Connection)
            {
                InitConnection(connection);
                return _ExecuteNonQuery(connection, query, parameters);
            }
        }

        /// <summary>
        /// Execute a Scalar Query
        /// </summary>
        /// <param name="connection">Connection to execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Single result from Query</returns>
        internal object ExecuteScalar(String query, params object[] parameters)
        {
            using (var connection = Connection)
            {
                InitConnection(connection);
                return _ExecuteScalar(connection, query, parameters);
            }
        }

        /// <summary>
        /// Execute a Reader Query
        /// </summary>
        /// <param name="connection">Connection to invoke on</param>
        /// <param name="query">Query to execute</param>
        /// <param name="handler">Handler to invoke for each record</param>
        internal void ExecuteReader(String query, DbRecordHandler handler, params object[] parameters)
        {
            using (var connection = Connection)
            {
                InitConnection(connection);
                _ExecuteReader(connection, query, handler, parameters);
            }
        }

        internal enum DatabaseAttributes
        {
            Nothing = 0,
            MySQL = 1,
            SQLite = 2,
            MyISAM = 4
        }

        internal delegate void DbRecordHandler(IDataRecord record);

    }
}
