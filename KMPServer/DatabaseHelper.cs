using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace KMPServer
{
    /// <summary>
    /// Database Helper taking the manual parts of queries away
    /// </summary>
    internal class DatabaseHelper
    {
        #region Constants
        private const String SQLITE_INIT_SQL = "PRAGMA auto_vacuum = 1;PRAGMA synchronous = 0;";
        #endregion

        #region Diagnostics
        private Stopwatch stateChangeWatch = new Stopwatch();
        private Stopwatch queryWatch = new Stopwatch();
        private int connectionOpenCount = 0;
        private int connectionQueryCount = 0;
        private DbConnection ReadRefDb = null;

        public TimeSpan TimeSpentChangingState
        {
            get { return stateChangeWatch.Elapsed; }
        }

        public TimeSpan TimeSpentOnQuery
        {
            get { return queryWatch.Elapsed; }
        }

        public int TotalServicedQueries
        {
            get { return connectionQueryCount; }
        }

        public int TotalDatabaseOpens
        {
            get { return connectionOpenCount; }
        }

        #endregion

        #region Static Helpers
        /// <summary>
        /// Create default SQLite connection
        /// </summary>
        /// <param name="filePath">DB file path</param>
        /// <returns>DatabaseHelper for Database</returns>
        internal static DatabaseHelper CreateForSQLite(String filePath)
        {
            return new DatabaseHelper("Data Source=" + filePath + "; Pooling=true; Max Pool Size=100;", DatabaseAttributes.SQLite | DatabaseAttributes.KeepRef);
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
        #endregion

        #region Properties
        /// <summary>
        /// Connection String used for database connection.
        /// Set in Constructor. Read-Only
        /// </summary>
        internal String ConnectionString { get; private set; }

        /// <summary>
        /// Database Attributes for the connection
        /// </summary>
        internal DatabaseAttributes Attributes { get; private set; }
        #endregion

        /// <summary>
        /// The amount of queries currently using the database
        /// </summary>
        /// <remarks>Syncronize access to this integer!</remarks>
        private int DatabaseUsers = 0;

        /// <summary>
        /// Create a new Database Helper
        /// </summary>
        /// <param name="ConnectionString">Database Connection String</param>
        internal DatabaseHelper(String ConnectionString, DatabaseAttributes attributes)
        {
            this.ConnectionString = ConnectionString;
            this.Attributes = attributes;
        }


        #region Core Connection Management

        /// <summary>
        /// If pooling is enabled. Pools the connection
        /// </summary>
        /// <param name="connection">Connection to pool</param>
        /// <returns>If pooling is disabled, returns it's only argument. Else it returns a pooled connection</returns>
        private DbConnection PoolNewConnection(DbConnection connection)
        {
            // Currently not implemented. Wont implement until more tests
            return connection;
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
                // Keep a single connection object during a reader operation
                if ((Attributes & DatabaseAttributes.KeepRef) == DatabaseAttributes.KeepRef && ReadRefDb != null && ReadRefDb.State == ConnectionState.Open) return ReadRefDb;
                stateChangeWatch.Start();
                switch (Attributes & (DatabaseAttributes.SQLite | DatabaseAttributes.MySQL))
                {
                    case DatabaseAttributes.MySQL:
                        stateChangeWatch.Stop();
                        return PoolNewConnection(new MySqlConnection(ConnectionString));
                    case DatabaseAttributes.SQLite:
                        stateChangeWatch.Stop();
                        return PoolNewConnection(new SQLiteConnection(ConnectionString));
                }
                return null;
            }
        }

        // Do I use a lock? Monitor perhaps? Or I could just use a counter and interlock
        /// <summary>
        /// Database is in use 
        /// </summary>
        private void UseDB()
        {
            Interlocked.Increment(ref DatabaseUsers);
        }

        /// <summary>
        /// Database is no longer in use
        /// </summary>
        /// <returns>Passthrough object</returns>
        private T Finish<T>(T o, DbConnection conn)
        {
            if (Interlocked.Decrement(ref DatabaseUsers) == 0)
            {
                if(ReadRefDb != conn && conn != null)
                    conn.Dispose();
            }
            return o;
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
                    stateChangeWatch.Start();
                    connection.Open();
                    connectionOpenCount++;
                    stateChangeWatch.Stop();
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
            connectionQueryCount++;
            return cmdObj;
        }
        #endregion

        #region Private Core Methods
        /// <summary>
        /// Execute a NonQuery 
        /// </summary>
        /// <param name="connection">Connection to Execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Rows affected</returns>
        private int _ExecuteNonQuery(DbConnection connection, String query, params object[] parameters)
        {
            try
            {
                queryWatch.Start();
                using (var command = CreateCommand(connection, query, parameters))
                {
                    return command.ExecuteNonQuery();
                }
            }
            finally
            {
                queryWatch.Stop();
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
            try
            {
                queryWatch.Start();
                using (var command = CreateCommand(connection, query, parameters))
                {
                    return command.ExecuteScalar();
                }
            }
            finally
            {
                queryWatch.Stop();
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
            try
            {
                if ((Attributes & DatabaseAttributes.KeepRef) == DatabaseAttributes.KeepRef) ReadRefDb = connection;
                queryWatch.Start();
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
            finally
            {
                if ((Attributes & DatabaseAttributes.KeepRef) == DatabaseAttributes.KeepRef) ReadRefDb = null;
                queryWatch.Stop();
            }
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Execute a NonQuery 
        /// </summary>
        /// <param name="connection">Connection to Execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Rows affected</returns>
        internal int ExecuteNonQuery(String query, params object[] parameters)
        {
            UseDB();
            var connection = Connection;
            InitConnection(connection);
            return Finish(_ExecuteNonQuery(connection, query, parameters), connection);
        }

        /// <summary>
        /// Execute a Scalar Query
        /// </summary>
        /// <param name="connection">Connection to execute on</param>
        /// <param name="query">Query to execute</param>
        /// <returns>Single result from Query</returns>
        internal object ExecuteScalar(String query, params object[] parameters)
        {
            UseDB();
            var connection = Connection;
            InitConnection(connection);
            return Finish(_ExecuteScalar(connection, query, parameters), connection);
        }

        /// <summary>
        /// Execute a Reader Query
        /// </summary>
        /// <param name="connection">Connection to invoke on</param>
        /// <param name="query">Query to execute</param>
        /// <param name="handler">Handler to invoke for each record</param>
        internal void ExecuteReader(String query, DbRecordHandler handler, params object[] parameters)
        {
            UseDB();
            var connection = Connection;
            InitConnection(connection);
            _ExecuteReader(connection, query, handler, parameters);
            Finish<object>(null, connection);
        }

        #endregion

        #region DatabaseAttributes Enum
        internal enum DatabaseAttributes
        {
            Nothing = 0,
            MySQL = 1,
            SQLite = 2,
            /// <summary>
            /// Currently not used. Signifies the use of the MyISAM engine in MySQL
            /// </summary>
            MyISAM = 4,
            /// <summary>
            /// Tells the Database Helper to pool the connection and not close it immediately
            /// </summary>
            Pool = 8,
            /// <summary>
            /// Tells the Database Helper to use one connection object while a reader is executing
            /// </summary>
            KeepRef = 16
        }

        #endregion 

        internal delegate void DbRecordHandler(IDataRecord record);

        public override string ToString()
        {
            return String.Format("DatabaseHelper connected to {0}. Total Queries: {1}, Total State Changes: {2}, Time Spent On Query: {3}, Time Spent On State: {4}, Avg. Query: {5}, Avg. State Change: {6}",
                ConnectionString, TotalServicedQueries, TotalDatabaseOpens, TimeSpentOnQuery, TimeSpentChangingState,
                 new TimeSpan(TimeSpentOnQuery.Ticks / TotalServicedQueries == 0 ? 1 : TotalServicedQueries), new TimeSpan(TimeSpentChangingState.Ticks / TotalDatabaseOpens == 0 ? 1 : TotalDatabaseOpens));
        }

    }
}
