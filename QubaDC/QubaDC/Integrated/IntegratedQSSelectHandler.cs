using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Utility;
using QubaDC.Restrictions;
using System.Data;

namespace QubaDC.Integrated
{
    public class IntegratedQSSelectHandler : QueryStoreSelectHandler
    {
        //        public override QueryStoreSelectResult HandleSelect(SelectOperation s, SchemaManager schemaManager, DataConnection con, GlobalUpdateTimeManager timeManager, CRUDVisitor cRUDHandler, QueryStore qs)
        //        {
        //            //3.Open transaction and lock tables(I)
        //            //4.Execute(original) query and retrieve subset.
        //            //5.Assign the last global update timestamp to the query (R7).
        //            //6.Close transaction and unlock tables(A) 

        //            //PS
        //            //We do not need to lock any tables here as we are querying on the historic database
        //            //If we want to, we COULD lock and do the query like the others
        //            //We choose here not to lock

        //            //Question is ... if we do not lock what is global update timestamp?
        //            //We query the global update timestampt and get the max from it
        //            //Then we get which schema was active there and get the history tables for this one            
        //            QueryStoreSelectResult execResult = null;
        //            if (s.GetAllSelectedTables().Any(x => String.IsNullOrWhiteSpace(x.TableAlias)))
        //                throw new InvalidOperationException("Table Alias needed on all tables in hybrid");
        //            con.DoTransaction((trans, c) =>
        //            {
        //                //What to do here:
        //                //0.) Get last updated Timestamp
        //                var lastGlobalUpdate = timeManager.GetLatestUpdate();
        //                var queryTime = lastGlobalUpdate.DateTime;

        //                //a.) copy the operation
        //                var newOperation = JsonSerializer.CopyItem(s);
        //                //b.) add, that we only query the current table
        //                List<Restriction> TimeStampRestrictions = new List<Restriction>();
        //                SchemaInfo SchemaInfo = schemaManager.GetSchemaActiveAt(lastGlobalUpdate.DateTime);
        //                System.Diagnostics.Debug.WriteLine(String.Join(",", SchemaInfo.Schema.Tables.Select(x => x.Table.AddTimeSetGuid)));

        //                Dictionary<String, Guid?> table_to_ids = new Dictionary<String, Guid?>();
        //                foreach (var selectedTable in newOperation.GetAllSelectedTables())
        //                {
        //                    var foundTabl =  SchemaInfo.Schema.FindTable(selectedTable);
        //                    table_to_ids.Add(selectedTable.TableAlias, foundTabl.Table.AddTimeSetGuid);
        //                    OperatorRestriction startTs = new OperatorRestriction()
        //                    {
        //                        LHS = new ColumnOperand()
        //                        {
        //                            Column = new ColumnReference()
        //                            {
        //                                ColumnName = SeparatedConstants.StartTS,
        //                                TableReference = selectedTable.TableAlias
        //                            }
        //                        },
        //                        Op = RestrictionOperator.LET
        //                        ,
        //                        RHS = new DateTimeRestrictionOperand()
        //                        {
        //                            Value = queryTime
        //                        },
        //                    };
        //                    TimeStampRestrictions.Add(startTs);
        //                }
        //                //c.) add timestamp parts to it
        //                if (newOperation.Restriction != null)
        //                    TimeStampRestrictions.Add(newOperation.Restriction);

        //                newOperation.Restriction = new AndRestriction() { Restrictions = TimeStampRestrictions.ToArray() };


        //                //d.) render it and return

        //                String select = cRUDHandler.RenderSelectOperation(newOperation);

        //                String originalSerialized = JsonSerializer.SerializeObject(s);
        //                String originalrenderd = cRUDHandler.RenderSelectOperation(s);

        //                String RewrittenSerialized = JsonSerializer.SerializeObject(newOperation);


        //                //Build Hash-Select


        //                String selectHash = cRUDHandler.RenderHashSelect(newOperation);
        //                String selectHashSerialized = cRUDHandler.RenderHashSelect(newOperation);

        //                //f.) Execute it
        //                DataTable normResult = null;
        //                DataTable hashTable = null;
        //                String hash = null;
        //                Guid guid = Guid.NewGuid();
        //                String time = cRUDHandler.CRUDRenderer.SerializeDateTime(queryTime);

        //                normResult = con.ExecuteQuery(select, c);
        //                hashTable = con.ExecuteQuery(selectHash, c);
        //                hash = hashTable.Select().First().Field<String>(0);
        //                String additionalInfos = JsonSerializer.SerializeObject(table_to_ids);
        //                String insert = qs.RenderInsert(originalrenderd, originalSerialized, RewrittenSerialized, select, time, hash, guid, selectHash, selectHashSerialized, additionalInfos);
        //                long? id = con.ExecuteInsert(insert, c);
        //                System.Diagnostics.Debug.WriteLine(id.Value);
        //                trans.Commit();
        //                execResult = new QueryStoreSelectResult()
        //                {
        //                    RewrittenSerialized = JsonSerializer.SerializeObject(newOperation),
        //                    RewrittenRenderd = select,
        //                    Result = normResult,
        //                    TimeStampOfExecution = queryTime
        //                ,
        //                    Hash = hash
        //                ,
        //                    GUID = guid
        //                };
        //            });

        //            return execResult;
        //        }

        //        public override QueryStoreReexecuteResult ReExecuteSelectFor(Guid gUID, QueryStore qs, DataConnection con, CRUDVisitor cRUDHandler, SchemaManager schemaManager)
        //        {
        //            String selectQueryStoreROw = qs.RenderSelectForQueryStore(gUID);
        //            DataTable t = con.ExecuteQuery(selectQueryStoreROw);
        //            if (t.Rows.Count != 1)
        //                throw new InvalidOperationException("Expected to get 1 Row, got: " + t.Rows.Count + " Could not do rexecue for guid: " + gUID.ToString());
        //            var row = t.Select().First();

        //            String hash = row.Field<String>("Hash");
        //            String query = row.Field<String>("QuerySerialized");
        //            DateTime queryTime = row.Field<DateTime>("Timestamp");
        //            String addInfo = row.Field<String>("AdditionalInformation");
        //            var table_to_ids = JsonSerializer.DeserializeObject<Dictionary<String, Guid?>>(addInfo);
        //            SelectOperation originalSelect = JsonSerializer.DeserializeObject<SelectOperation>(query);
        //            var SchemaInfo = schemaManager.GetSchemaActiveAt(queryTime);
        //            List<Restriction> TimeStampRestrictions = new List<Restriction>();

        //            foreach (var selectedTable in originalSelect.GetAllSelectedTables())
        //            {
        //                OperatorRestriction startTs = new OperatorRestriction()
        //                {
        //                    LHS = new ColumnOperand()
        //                    {
        //                        Column = new ColumnReference()
        //                        {
        //                            ColumnName = SeparatedConstants.StartTS,
        //                            TableReference = selectedTable.TableAlias
        //                        }
        //                    },
        //                    Op = RestrictionOperator.LET
        //                    ,
        //                    RHS = new DateTimeRestrictionOperand()
        //                    {
        //                        Value = queryTime
        //                    },
        //                };

        //                OperatorRestriction endTsLt = new OperatorRestriction()
        //                {
        //                    LHS = new DateTimeRestrictionOperand()
        //                    {
        //                        Value = queryTime
        //                    },
        //                    Op = RestrictionOperator.LT
        //    ,
        //                    RHS = new ColumnOperand()
        //                    {
        //                        Column = new ColumnReference()
        //                        {
        //                            ColumnName = SeparatedConstants.EndTS,
        //                            TableReference = selectedTable.TableAlias
        //                        }
        //                    },
        //                };
        //                OperatorRestriction endTSNull = new OperatorRestriction()
        //                {
        //                    LHS = new ColumnOperand()
        //                    {
        //                        Column = new ColumnReference()
        //                        {
        //                            ColumnName = SeparatedConstants.EndTS,
        //                            TableReference = selectedTable.TableAlias
        //                        }
        //                    },
        //                    Op = RestrictionOperator.IS
        //,
        //                    RHS = new LiteralOperand()
        //                    {
        //                        Literal = "NULL"
        //                    }
        //                };
        //                var OrRestriction = new OrRestriction();
        //                OrRestriction.Restrictions = new Restriction[] { endTsLt, endTSNull };
        //                var AndRestriction = new AndRestriction();
        //                AndRestriction.Restrictions = new Restriction[] { startTs, OrRestriction };
        //                TimeStampRestrictions.Add(AndRestriction);
        //            }
        //            //c.) add timestamp parts to it
        //            if (originalSelect.Restriction != null)
        //                TimeStampRestrictions.Add(originalSelect.Restriction);

        //            originalSelect.Restriction = new AndRestriction() { Restrictions = TimeStampRestrictions.ToArray() };


        //            SchemaInfo schemaAtExecutionTime = schemaManager.GetSchemaActiveAt(queryTime);
        //            SchemaInfo CurrentSchema = schemaManager.GetCurrentSchema();
        //            String hybridSelect = cRUDHandler.RenderHybridSelectOperation(originalSelect, schemaAtExecutionTime, CurrentSchema, table_to_ids);
        //            String hybridHashSelect = cRUDHandler.RenderHybridHashSelect(originalSelect, schemaAtExecutionTime, CurrentSchema, table_to_ids);


        //            DataTable normaResult = null;
        //            DataTable hashTable = null;
        //            con.DoTransaction((trans, c) =>
        //            {

        //                hashTable = con.ExecuteQuery(hybridHashSelect, c);
        //                String currentHash = hashTable.Select().First().Field<String>(0);
        //                if (currentHash != hash)
        //                    throw new InvalidOperationException("Hashes for GUID: " + gUID.ToString() + " Are not equal, HashSelect: " + hybridHashSelect + System.Environment.NewLine + "Select: " + query);
        //                normaResult = con.ExecuteQuery(hybridSelect, c);
        //                trans.Commit();
        //            });

        //            return new QueryStoreReexecuteResult()
        //            {
        //                GUID = gUID,
        //                Hash = hash,
        //                Result = normaResult
        //            };
        //        }
        public override QueryStoreSelectResult HandleSelect(SelectOperation s, SchemaManager schemaManager, DataConnection con, TableMetadataManager metaManager, CRUDVisitor cRUDHandler, QueryStore qs)
        {
            //3.Open transaction and lock tables(I)
            //4.Execute(original) query and retrieve subset.
            //5.Assign the last global update timestamp to the query (R7).
            //6.Close transaction and unlock tables(A) 

            //PS
            //We do not need to lock any tables here as we are querying on the historic database
            //If we want to, we COULD lock and do the query like the others
            //We choose here not to lock

            //Question is ... if we do not lock what is global update timestamp?
            //We query the global update timestampt and get the max from it
            //Then we get which schema was active there and get the history tables for this one            

            TableToLock[] aliasTables = s.GetAllSelectedTables().Select(x => new TableToLock { Alias = x.TableAlias, LockAsWrite = false, Name = cRUDHandler.CRUDRenderer.PrepareTable(x) }).ToArray();
            TableToLock[] metaTables = s.GetAllSelectedTables().Select(x => metaManager.GetMetaTableFor(x.TableSchema, x.TableName))
                                                               .Select(x => new TableToLock { Alias = null, LockAsWrite = false, Name = cRUDHandler.CRUDRenderer.PrepareTable(x) })
                                                               .ToArray();

            TableToLock[] tablesToLock = aliasTables.Union(metaTables).ToArray();

            Table[] MetaTables = s.GetAllSelectedTables()//.Select(x => metaManager.GetMetaTableFor(x.TableSchema, x.TableName))
                                .ToArray();

           // String[] tablesToLock = s.GetAllSelectedTables().Select(x => new String[] { cRUDHandler.CRUDRenderer.PrepareTable(x));

            //0.) Get last updated Timestamp

            Dictionary<String, Guid?> table_to_ids = null;
            DateTime queryTime = DateTime.Now;
            String originalrenderd = "";
            String originalSerialized = "";
            String RewrittenSerialized = "";
            String select = "";
            String selectHash = "";
            String selectHashSerialized = "";
            SelectOperation newOperation = null;
            DataTable normResult = null;
            Func<SchemaInfo, DateTime, String[]> RenderSelectStatements = (schema, dt) =>
             {
                 queryTime = dt;

                 //a.) copy the operation
                 newOperation = JsonSerializer.CopyItem(s);
                 //b.) change all tables to the respective history ones + build restrictions for it
                 List<Restriction> TimeStampRestrictions = new List<Restriction>();
                 SchemaInfo SchemaInfo = schema;
                 table_to_ids = new Dictionary<String, Guid?>();

                 foreach (var selectedTable in newOperation.GetAllSelectedTables())
                 {
                     var foundTabl = SchemaInfo.Schema.FindTable(selectedTable);
                     table_to_ids.Add(selectedTable.TableAlias, foundTabl.Table.AddTimeSetGuid);
                     //var histTable = SchemaInfo.Schema.FindHistTable(selectedTable).ToTable();
                     //selectedTable.TableName = histTable.TableName;
                     //selectedTable.TableSchema = histTable.TableSchema;
                     OperatorRestriction startTs = new OperatorRestriction()
                     {
                         LHS = new ColumnOperand()
                         {
                             Column = new ColumnReference()
                             {
                                 ColumnName = IntegratedConstants.StartTS,
                                 TableReference = selectedTable.TableAlias
                             }
                         },
                         Op = RestrictionOperator.LET
                          ,
                         RHS = new DateTimeRestrictionOperand()
                         {
                             Value = queryTime
                         },
                     };

                     OperatorRestriction endTsLt = new OperatorRestriction()
                     {
                         LHS = new DateTimeRestrictionOperand()
                         {
                             Value = queryTime
                         },
                         Op = RestrictionOperator.LT
         ,
                         RHS = new ColumnOperand()
                         {
                             Column = new ColumnReference()
                             {
                                 ColumnName = IntegratedConstants.EndTS,
                                 TableReference = selectedTable.TableAlias
                             }
                         },
                     };
                     OperatorRestriction endTSNull = new OperatorRestriction()
                     {
                         LHS = new ColumnOperand()
                         {
                             Column = new ColumnReference()
                             {
                                 ColumnName = IntegratedConstants.EndTS,
                                 TableReference = selectedTable.TableAlias
                             }
                         },
                         Op = RestrictionOperator.IS
     ,
                         RHS = new LiteralOperand()
                         {
                             Literal = "NULL"
                         }
                     };
                     var OrRestriction = new OrRestriction();
                     OrRestriction.Restrictions = new Restriction[] { endTsLt, endTSNull };
                     var AndRestriction = new AndRestriction();
                     AndRestriction.Restrictions = new Restriction[] { startTs, OrRestriction };
                     TimeStampRestrictions.Add(AndRestriction);
                 }
                 //c.) add timestamp parts to it
                 if (newOperation.Restriction != null)
                     TimeStampRestrictions.Add(newOperation.Restriction);

                 newOperation.Restriction = new AndRestriction() { Restrictions = TimeStampRestrictions.ToArray() };

                 //d.) render it and return



                 originalSerialized = JsonSerializer.SerializeObject(s);
                 originalrenderd = cRUDHandler.RenderSelectOperation(s);

                 select = cRUDHandler.RenderSelectOperation(newOperation);
                 RewrittenSerialized = JsonSerializer.SerializeObject(newOperation);

                 selectHash = cRUDHandler.RenderHashSelect(newOperation);
                 selectHashSerialized = JsonSerializer.SerializeObject(newOperation);

                 return new String[] { selectHash, select };
             };

            //f.) Execute it

            String hash = null;
            Guid guid = Guid.NewGuid();
            String time = cRUDHandler.CRUDRenderer.SerializeDateTime(queryTime);   


            Func<DataTable, DataTable, String> RenderInsert = (hashTable, SelectTable) =>
              {
                  normResult = SelectTable;
                  hash = hashTable.Select().First().Field<String>(0);
                  String additionalInfos = JsonSerializer.SerializeObject(table_to_ids);

                  String insert = qs.RenderInsert(originalrenderd, originalSerialized, RewrittenSerialized, select, time, hash, guid, selectHash, selectHashSerialized, additionalInfos);
                  return insert;
              };

            Action<String> log = (logst) => { System.Diagnostics.Debug.WriteLine(logst); };
            IntegratedQSSelectExecuter executer = new IntegratedQSSelectExecuter();
            long id = executer.ExecuteStatementsOnLockedTables(RenderInsert, RenderSelectStatements, tablesToLock,con,cRUDHandler.CRUDRenderer,schemaManager, MetaTables, metaManager, log);


            var execResult = new QueryStoreSelectResult()
            {
                RewrittenSerialized = JsonSerializer.SerializeObject(newOperation),
                RewrittenRenderd = select,
                Result = normResult,
                TimeStampOfExecution = queryTime
                ,
                Hash = hash
                ,
                GUID = guid

            };

            return execResult;
        }

        public override QueryStoreReexecuteResult ReExecuteSelectFor(Guid gUID, QueryStore qs, DataConnection con, CRUDVisitor cRUDHandler, SchemaManager schemaManager)
        {
            //Note => Need to check if current table exists at the time of execution!!

            String selectQueryStoreROw = qs.RenderSelectForQueryStore(gUID);
            DataTable t = con.ExecuteQuery(selectQueryStoreROw);
            if (t.Rows.Count != 1)
                throw new InvalidOperationException("Expected to get 1 Row, got: " + t.Rows.Count + " Could not do rexecue for guid: " + gUID.ToString());
            var row = t.Select().First();

            String hash = row.Field<String>("Hash");
            String query = row.Field<String>("ReWrittenQuerySerialized");
            String querySerialized = row.Field<String>("HashSelectSerialized");

            DateTime queryTime = row.Field<DateTime>("Timestamp");
            SchemaInfo schemaAtExecutionTime = schemaManager.GetSchemaActiveAt(queryTime);
            SchemaInfo CurrentSchema = schemaManager.GetCurrentSchema();
            String originalSerialized = row.Field<String>("ReWrittenQuerySerialized");
            SelectOperation originalSelect = JsonSerializer.DeserializeObject<SelectOperation>(originalSerialized);
            String addInfo = row.Field<String>("AdditionalInformation");
            var table_to_ids = JsonSerializer.DeserializeObject<Dictionary<String, Guid?>>(addInfo);

            String hybridSelect = cRUDHandler.RenderHybridSelectOperation(originalSelect, schemaAtExecutionTime, CurrentSchema, table_to_ids);
            String hybridHashSelect = cRUDHandler.RenderHybridHashSelect(originalSelect, schemaAtExecutionTime, CurrentSchema, table_to_ids);



            DataTable normaResult = null;
            DataTable hashTable = null;
            con.DoTransaction((trans, c) =>
            {

                hashTable = con.ExecuteQuery(hybridHashSelect, c);
                String currentHash = hashTable.Select().First().Field<String>(0);
                if (currentHash != hash)
                    throw new InvalidOperationException("Hashes for GUID: " + gUID.ToString() + " Are not equal, HashSelect: " + querySerialized + System.Environment.NewLine + "Select: " + query);
                normaResult = con.ExecuteQuery(hybridSelect, c);
                trans.Commit();
            });

            return new QueryStoreReexecuteResult()
            {
                GUID = gUID,
                Hash = hash,
                Result = normaResult
            };
        }
    }
}

