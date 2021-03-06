﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Separated.SMO;
using QubaDC.Integrated.SMO;

namespace QubaDC.Integrated
{
    public class IntegratedSMOHandler : SMOVisitor
    {

        //public override void Visit(RenameTable renameTable)
        //{
        //    HybridRenameTableHandler h = new HybridRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(renameTable);            
        //}

        //public override void Visit(PartitionTable partitionTable)
        //{
        //    HybridPartitionTableHandler h = new HybridPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(partitionTable);
        //}

        //public override void Visit(DropTable dropTable)
        //{
        //    HybridDropTableHandler h = new HybridDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(dropTable);
        //}

        //public override void Visit(CreateTable createTable)
        //{
        //    HybridCreateTableHandler h = new HybridCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(createTable);
        //}

        //public override void Visit(DropColumn dropColumn)
        //{
        //    HybridDropColumnHandler h = new HybridDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(dropColumn);
        //}

        //public override void Visit(CopyTable copyTable)
        //{
        //    HybridCopyTableHandler h = new HybridCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(copyTable);
        //}

        //public override void Visit(DecomposeTable decomposeTable)
        //{
        //    HybridDecomposeTableHandler h = new HybridDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(decomposeTable);
        //}

        //public override void Visit(JoinTable joinTable)
        //{
        //    HybridJoinTableHandler h = new HybridJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(joinTable);
        //}

        //public override void Visit(RenameColumn renameColumn)
        //{
        //    HybridRenameColumnHandler h = new HybridRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(renameColumn);            
        //}

        //public override void Visit(MergeTable mergeTable)
        //{
        //    HybridMergeTableHandler h = new HybridMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(mergeTable);
        //}

        //public override void Visit(AddColum addColum)
        //{
        //    HybridAddColumnHandler h = new HybridAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer);
        //    h.Handle(addColum);
        //}
        public override void Visit(RenameTable renameTable)
        {
            var h = new IntegratedRenameTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(renameTable);
        }

        public override void Visit(PartitionTable partitionTable)
        {
            var h = new IntegratedPartitionTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(partitionTable);
        }

        public override void Visit(DropTable dropTable)
        {
            var h = new IntegratedDropTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(dropTable);
        }

        public override void Visit(CreateTable createTable)
        {
            IntegratedCreateTableHandler h = new IntegratedCreateTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(createTable);
        }

        public override void Visit(DropColumn dropColumn)
        {
            var h = new IntegratedDropColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(dropColumn);
        }

        public override void Visit(CopyTable copyTable)
        {
            var h = new IntegratedCopyTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(copyTable);
        }

        public override void Visit(DecomposeTable decomposeTable)
        {
            var h = new IntegratedDecomposeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(decomposeTable);
        }

        public override void Visit(JoinTable joinTable)
        {
            var h = new IntegratedJoinTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer,this.MetaManager);
            h.Handle(joinTable);
        }

        public override void Visit(RenameColumn renameColumn)
        {
            var h = new IntegratedRenameColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(renameColumn);
        }

        public override void Visit(MergeTable mergeTable)
        {
            var h = new IntegratedMergeTableHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(mergeTable);
        }

        public override void Visit(AddColum addColum)
        {
            IntegratedAddColumnHandler h = new IntegratedAddColumnHandler(this.DataConnection, this.SchemaManager, this.SMORenderer, this.MetaManager);
            h.Handle(addColum);
        }
    }
}
