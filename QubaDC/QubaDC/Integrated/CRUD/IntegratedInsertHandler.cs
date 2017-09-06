using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    class IntegratedInsertHandler
    {
        public IntegratedInsertHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender, GlobalUpdateTimeManager timeManager)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
            this.timeManager = timeManager;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public GlobalUpdateTimeManager timeManager { get; private set; }

        internal void HandleInsert(InsertOperation insertOperation)
        {
            //String insertIntoBaseTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
            //this.DataConnection.ExecuteQuery(insertIntoBaseTable);
            //          this.DataConnection.
            this.DataConnection.AquiereOpenConnection(con =>
            {
                String[] tables = new string[]
                {
                   insertOperation.InsertTable.TableSchema+"."+insertOperation.InsertTable.TableName,
                   timeManager.GetTableName()
                };
                String[] tablesWithWrite = tables.Select(x => x + " WRITE").ToArray();
                String lockTables = String.Format("LOCK TABLES {0}", String.Join(",", tablesWithWrite)) + ";";
                String[] setupAndAquireLock = new String[] {
                @"SET autocommit=0;",
                lockTables
            };
                try
                {
                    foreach (var setupSql in setupAndAquireLock)
                        this.DataConnection.ExecuteNonQuerySQL(setupSql, con);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not aquire locks for:" + String.Join(",", tables), e);
                }

                try
                {
                    //We now have our locks
                    DateTime t = System.DateTime.Now;
                    insertOperation.ColumnNames = insertOperation.ColumnNames.Concat(new String[] { IntegratedConstants.StartTS, IntegratedConstants.EndTS }).ToArray();
                    insertOperation.ValueLiterals = insertOperation.ValueLiterals.Concat(new String[]
                    {
                     MySQLDialectHelper.RenderDateTime(t),
                     "null"
                    }).ToArray();
                    String insertToTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
                    String insertToGlobalUpdate = this.CRUDRenderer.RenderInsert(this.timeManager.GetTable(),
                        new String[] {"Operation","Timestamp"},
                        new String[] { String.Format("'insert on {0}'", this.timeManager.GetTable().TableName), MySQLDialectHelper.RenderDateTime(t) }
                        );
                    System.Diagnostics.Debug.WriteLine(insertToTable);
                    this.DataConnection.ExecuteNonQuerySQL(insertToTable,con);
                    this.DataConnection.ExecuteNonQuerySQL(insertToGlobalUpdate,con);
                    this.DataConnection.ExecuteNonQuerySQL("COMMIT;",con);
                    this.DataConnection.ExecuteNonQuerySQL("UNLOCK TABLES;",con);

                }
                catch (Exception e)
                {
                    this.DataConnection.ExecuteNonQuerySQL("ROLLBACK;");
                    this.DataConnection.ExecuteNonQuerySQL("UNLOCK TABLES;");
                    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                }



 //               String[] Inserts = new String[]
 //               {
 //                   insertToTable,

 //               @"	INSERT INTO `testing_scripts`.`updatetable`
	//(`comment`,
	//`updatetie`)
	//VALUES
	//('some value',
	//" + MySQLDialectHelper.RenderDateTime(t) + "); "
 //               };

 //               String[] BackupStrings = new String[]
 //               {
 //               "ROLLBACK;",
 //               "UNLOCK TABLES;"
 //               };

 //               String[] CommitStrings = new String[]
 //               {
 //               "COMMIT;",
 //               "UNLOCK TABLES;"
 //               };



 //               //try
 //               //{
 //               //    foreach (var insert in Inserts)
 //               //        c.ExecuteNonQuerySQL(insert, con);
 //               //    foreach (var commit in CommitStrings)
 //               //        c.ExecuteNonQuerySQL(commit, con);
 //               //}
 //               //catch (Exception e)
 //               //{
 //               //    foreach (var insert in BackupStrings)
 //               //        c.ExecuteNonQuerySQL(insert, con);
 //               //    return;
 //               //}
            });

        }
    }
}
