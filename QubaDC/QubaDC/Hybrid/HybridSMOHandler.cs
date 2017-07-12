using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Separated.SMO;
using QubaDC.Hybrid.SMO;

namespace QubaDC.Hybrid
{
    public class HybridSMOHandler : SMOVisitor
    {
        //internal override void Visit(RenameTable renameTable)
        //{
        //    throw new NotImplementedException();
        //    //SeparatedRenameTableHandler h = new SeparatedRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    //h.Handle(renameTable);
        //}

        //internal override void Visit(PartitionTable partitionTable)
        //{
        //    SeperatedPartitionTableHandler h = new SeperatedPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(partitionTable);
        //}

        //internal override void Visit(DropTable dropTable)
        //{
        //    SeparatedDropTableHandler h = new SeparatedDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(dropTable);
        //}

        //internal override void Visit(CreateTable createTable)
        //{
        //    SeparatedCreateTableHandler h = new SeparatedCreateTableHandler(this.DataConnection,this.SchemaManager, this.SMORenderer);
        //    h.Handle(createTable);
        //}

        //internal override void Visit(CopyTable copyTable)
        //{
        //    SeparatedCopyTableHandler h = new SeparatedCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(copyTable);
        //}

        //internal override void Visit(DecomposeTable decomposeTable)
        //{
        //    SeparatedDecomposeTableHandler h = new SeparatedDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(decomposeTable);
        //}

        //internal override void Visit(JoinTable joinTable)
        //{
        //    SeparatedJoinTableHandler h = new SeparatedJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(joinTable);
        //}

        //internal override void Visit(RenameColumn renameColumn)
        //{
        //    SeparatedRenameColumnHandler h = new SeparatedRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(renameColumn);
        //}

        //internal override void Visit(MergeTable mergeTable)
        //{
        //    SeparatedMergeTableHandler h = new SeparatedMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(mergeTable);
        //}

        //internal override void Visit(AddColum addColum)
        //{
        //    SeparatedAddColumnHandler h = new SeparatedAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(addColum);
        //}

        //internal override void Visit(DropColumn dropColumn)
        //{
        //    SepearatedDropColumnHandler h = new SepearatedDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(dropColumn);            
        //}
        internal override void Visit(RenameTable renameTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(PartitionTable partitionTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DropTable dropTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(CreateTable createTable)
        {
            HybridCreateTableHandler h = new HybridCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
            h.Handle(createTable);
        }

        internal override void Visit(DropColumn dropColumn)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(CopyTable copyTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DecomposeTable decomposeTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(JoinTable joinTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(RenameColumn renameColumn)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(MergeTable mergeTable)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(AddColum addColum)
        {
            throw new NotImplementedException();
        }
    }
}
