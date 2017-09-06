using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    public class IntegratedCRUDExecuter
    {
        public static void ExecuteStatementsOnLockedTables(Func<String[]> RenderStatements, String[] locktables, DataConnection DataConnection, CRUDRenderer crudRenderer)
        {
            DataConnection.AquiereOpenConnection(con =>
            {
                String[] lockTableStatements = crudRenderer.RenderLockTables(locktables);
                try
                {
                    foreach (var setupSql in lockTableStatements)
                        DataConnection.ExecuteNonQuerySQL(setupSql, con);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not aquire locks for:" + String.Join(",", locktables), e);
                }

                try
                {
                    String[] statements = RenderStatements();
                    String[] success = crudRenderer.RenderCommitAndUnlock();
                    foreach (var stmt in statements)
                        DataConnection.ExecuteNonQuerySQL(stmt, con);
                    DataConnection.ExecuteNonQuerySQL(success[0], con);
                    DataConnection.ExecuteNonQuerySQL(success[1], con);

                }
                catch (Exception e)
                {
                    String[] rollbackAndUnlock = crudRenderer.RenderRollBackAndUnlock();
                    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[0]);
                    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[1]);
                    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                }

            });
        }
    }
}
