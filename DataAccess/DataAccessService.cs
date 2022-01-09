using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace SwaggerWebAPI.DataAccess
{
    public class DataAccessService
    {
        private string _connectionStr = string.Empty;
        private string _connectionSecStr = string.Empty;
        private int _connectionTimeout = 60;
    
        /// <summary>
        /// Initializes a new instance of the <see cref="DataAccessService"/> class.
        /// </summary>
        /// <param name="connectionString">Connection string used to connect to the database</param>
        public DataAccessService( string connectionString , string connectionSecStr)
        {
            _connectionStr = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _connectionSecStr = connectionSecStr ?? throw new ArgumentNullException(nameof(connectionSecStr));
        }


        #region Query 系列

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="param">查詢參數物件</param>
        /// <param name="timeoutSecs">SQL執行Timeout秒數</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>x
        public async Task<IEnumerable<TReturn>> QueryAsyncwithTimeoutTime<TReturn>(string querySql, object param = null, int timeoutSecs = 20, bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync<TReturn>(querySql, param, null, timeoutSecs, commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="param">查詢參數物件</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>x
        public async Task<IEnumerable<TReturn>> QueryAsync<TReturn>(string querySql, object param = null, bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync<TReturn>(querySql, param, null, _connectionTimeout, commandType).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數物件</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string querySql, Func<TFirst, TSecond, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType, commandTimeout: _connectionTimeout).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TFourth">回覆的資料類型4</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TFourth">回覆的資料類型4</typeparam>
        /// <typeparam name="TFifth">回覆的資料類型5</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TFourth">回覆的資料類型4</typeparam>
        /// <typeparam name="TFifth">回覆的資料類型5</typeparam>
        /// <typeparam name="TSixth">回覆的資料類型6</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TFourth">回覆的資料類型4</typeparam>
        /// <typeparam name="TFifth">回覆的資料類型5</typeparam>
        /// <typeparam name="TSixth">回覆的資料類型6</typeparam>
        /// <typeparam name="TSeventh">回覆的資料類型7</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢第一筆資料
        /// (無結果回傳Null)
        /// </summary>
        /// <typeparam name="TResult">回傳的資料型態</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="param">查詢參數</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<TResult> QueryFirstOrDefaultAsync<TResult>(string querySql, object param = null, bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryFirstOrDefaultAsync<TResult>(querySql, param, null, _connectionTimeout, commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢全部資料
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TResult>> QueryAll<TResult>()
            where TResult : class
        {
            using (IDbConnection con = GetDbConnection())
            {
                return await con.GetAllAsync<TResult>(null, _connectionTimeout).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 依主鍵Primary Key取回單筆資料
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <param name="id">Primary Key</param>
        /// <returns>資料物件</returns>
        public async Task<TResult> QueryById<TResult>(int id)
            where TResult : class
        {
            using (IDbConnection con = GetDbConnection())
            {
                return await con.GetAsync<TResult>(id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 依主鍵Primary Key取回單筆資料
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <param name="id">Primary Key</param>
        /// <returns>資料物件</returns>
        public async Task<TResult> QueryById<TResult>(long id)
            where TResult : class
        {
            using (IDbConnection con = GetDbConnection())
            {
                return await con.GetAsync<TResult>(id).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 依主鍵Primary Key取回單筆資料
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <param name="id">Primary Key</param>
        /// <returns>資料物件</returns>
        public async Task<TResult> QueryById<TResult>(string id)
            where TResult : class
        {
            using (IDbConnection con = GetDbConnection())
            {
                return await con.GetAsync<TResult>(id).ConfigureAwait(false);
            }
        }

        #endregion

        #region Insert 系列


        /// <summary>
        /// 測試 Transcation Scope
        /// </summary>
        /// <returns></returns>
        public bool TestInsertWithTranscation()
        {
            using (TransactionScope scope = new TransactionScope())
            using (IDbConnection con = GetDbConnection())
            {
                try
                {
                    //insert to PXLoading 1 in TCPS Database
                    string ins1 = @"INSERT INTO [dbo].[PXLoading] ([PlanCode],[BandStage],[Limits],[BandValue] 
                                                     ,[LastModifyTime],[LastModifyUser])
                                     VALUES('FVLB1N',1,66500,0.03,GETDATE(),'TESTUSER');";
                    con.Execute(ins1);

                    //insert to PXCorridor 2 in TCPS Database, this one will have error
                    string ins2 = @"INSERT INTO [dbo].[PXCorridor]([Age],[CorridorRule],[LastModifyTime],
                                    [LastModifyUser]) VALUES ( 10,1.9,GETDATE(),'TESTUSER');";

                    con.Execute(ins2);
                    //if success.. complete trans
                    scope.Complete();

                    return true;
                }
                catch (Exception e)
                {
                    e.Message.ToString();
                    // Not needed any rollback, if you don't call Complete
                    // a rollback is automatic exiting from the using block
                    // con.BeginTransaction().Rollback();
                    return false;
                }

            }
        }


        /// <summary>
        /// 測試 Transcation Scope with 2 DB
        /// </summary>
        /// <returns></returns>
        public bool TestInsertWithDiffDB()
        {
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    using (IDbConnection con = GetDbConnection())
                    {
                        //insert to PXLoading 1 in TCPS Database
                        string ins1 = @"INSERT INTO [dbo].[PXCorridor]([Age],[CorridorRule],[LastModifyTime],
                                        [LastModifyUser]) VALUES ( 12,1.9,GETDATE(),'TESTUSER');";
                        con.Execute(ins1);

                    }

                    using (IDbConnection con2 = GetDbConnection())
                    {
                        //insert to PXCorridor 2 in TCPS Database, this one will have error Age is a numberic, but 
                        // i try insert ABC instead
                        string ins2 = @"INSERT INTO [dbo].[PXLoading] ([PlanCode],[BandStage],[Limits],[BandValue] 
                                                             ,[LastModifyTime],[LastModifyUser])
                                             VALUES('FVLB1N',10,66500,0.03,GETDATE(),'TESTUSER');";
                     

                        con2.Execute(ins2);
                    }

                    //if success.. complete trans
                    scope.Complete();

                    return true;
                }
                catch (Exception e)
                {
                    e.Message.ToString();
                    // Not needed any rollback, if you don't call Complete
                    // a rollback is automatic exiting from the using block
                    // con.BeginTransaction().Rollback();
                    return false;
                }
            }

        }

        /// <summary>
        /// 測試 Transcation Scope with 2 server
        /// </summary>
        /// <returns></returns>
        public bool TestInsertWithMultiServer()
        {
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    using (IDbConnection con = GetDbConnection())
                    {
                        //insert to PXLoading 1 in TCPS Database
                        string ins1 = @"INSERT INTO [dbo].[PXCorridor]([Age],[CorridorRule],[LastModifyTime],
                                        [LastModifyUser]) VALUES ( 12,1.9,GETDATE(),'TESTUSER');";
                        con.Execute(ins1);

                    }

                    using (IDbConnection con2 = GetStrDbConnection())
                    {
                        //insert to PXCorridor 2 in TCPS Database, this one will have error Age is a numberic, but 
                        // i try insert ABC instead
                        string ins2 = @"INSERT INTO [dbo].[PXLoading] ([PlanCode],[BandStage],[Limits],[BandValue] 
                                                             ,[LastModifyTime],[LastModifyUser])
                                             VALUES('FVLB1N',10,66500,0.03,GETDATE(),'TESTUSER');";


                        con2.Execute(ins2);
                    }

                    //if success.. complete trans
                    scope.Complete();

                    return true;
                }
                catch (Exception e)
                {
                    e.Message.ToString();
                    // Not needed any rollback, if you don't call Complete
                    // a rollback is automatic exiting from the using block
                    // con.BeginTransaction().Rollback();
                    return false;
                }
            }

        }


        /// <summary>
        /// 新增三個不同類型資料
        /// </summary>
        /// <typeparam name="T">新增資料物件Type</typeparam>
        /// <typeparam name="T2">新增資料物件Type2</typeparam>
        /// <typeparam name="T3">新增資料物件Type3</typeparam>
        /// <param name="insertEntities">新增物件</param>
        /// <param name="insertEntities2">新增物件2</param>
        /// <param name="insertEntities3">新增物件3</param>
        /// <returns></returns>
        public async Task<bool> InsertThreeAsync<T, T2, T3>(T insertEntities, T2 insertEntities2, T3 insertEntities3)
            where T : class
            where T2 : class
            where T3 : class
        {
            bool isOk = false;
            using (IDbConnection con = GetDbConnection())
            {
                using (var transaction = con.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                {
                    try
                    {
                        if (await con.InsertAsync(insertEntities, transaction, _connectionTimeout).ConfigureAwait(false) > 0)
                        {
                            if (await con.InsertAsync(insertEntities2, transaction, _connectionTimeout).ConfigureAwait(false) > 0)
                            {
                                if (await con.InsertAsync(insertEntities3, transaction, _connectionTimeout).ConfigureAwait(false) > 0)
                                {
                                    transaction.Commit();
                                    isOk = true;
                                }
                            }
                        }

                        if (!isOk)
                        {
                            transaction.Rollback();
                        }
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }

            return isOk;
        }


        /// <summary>
        /// 新增二個不同類型資料
        /// </summary>
        /// <typeparam name="T">新增資料物件Type</typeparam>
        /// <typeparam name="T2">新增資料物件Type2</typeparam>
        /// <param name="insertEntities">新增物件</param>
        /// <param name="insertEntities2">新增物件2</param>
        /// <returns></returns>
        public async Task<bool> InsertTwoAsync<T, T2>(T insertEntities, T2 insertEntities2) 
            where T : class
            where T2 : class
        {
            bool isOk = false;
            using (IDbConnection con = GetDbConnection())
            {
                using (var transaction = con.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                {
                    try
                    {
                        if (await con.InsertAsync(insertEntities, transaction, _connectionTimeout).ConfigureAwait(false) > 0)
                        {
                            if (await con.InsertAsync(insertEntities2, transaction, _connectionTimeout).ConfigureAwait(false) > 0)
                            {     
                                transaction.Commit();    
                                isOk = true;
                            }
                        }

                        if (!isOk)
                        {
                            transaction.Rollback();
                        }
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }

            return isOk;
        }

        #endregion

        #region Private 功能

        /// <summary>
        /// 建立資料庫連線
        /// </summary>
        /// <param name="readOnlyConnection">是否唯讀</param>
        /// <returns>資料庫連線</returns>
        private IDbConnection GetDbConnection(bool readOnlyConnection = false)
        {
            IDbConnection dBConnection;

          
            dBConnection = new SqlConnection(readOnlyConnection ? _connectionSecStr : _connectionStr);
          
       

            if (dBConnection.State != ConnectionState.Open)
            {
                dBConnection.Open();
            }

            return dBConnection;
        }



        /// <summary>
        /// 建立資料庫連線
        /// </summary>
        /// <param name="readOnlyConnection">是否唯讀</param>
        /// <returns>資料庫連線</returns>
        private IDbConnection GetStrDbConnection(bool readOnlyConnection = false)
        {
            IDbConnection dBConnection;


            dBConnection = new SqlConnection( _connectionSecStr);



            if (dBConnection.State != ConnectionState.Open)
            {
                dBConnection.Open();
            }

            return dBConnection;
        }

        /// <summary>
        /// GetSqlDbConnection
        /// </summary>
        /// <param name="readOnlyConnection">readOnlyConnection</param>
        /// <returns></returns>
        private IDbConnection GetSqlDbConnection(bool readOnlyConnection = false)
        {
            IDbConnection dBConnection;

            dBConnection = new SqlConnection(readOnlyConnection ? _connectionSecStr : _connectionStr);

            if (dBConnection.State != ConnectionState.Open)
            {
                dBConnection.Open();
            }

            return dBConnection;
        }

   
        #endregion
    }

    /// <summary>
    /// 定義Model的類型
    /// </summary>
    public class DBColumnType
    {
        /// <summary>
        /// char型態
        /// </summary>
        public const string Char = "char";

        /// <summary>
        /// char型態
        /// </summary>
        public const string String = "string";
    }
}

