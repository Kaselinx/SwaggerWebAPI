using System;
using System.Collections.Generic;
using System.Text;

namespace DataAccess.ColumnAttribute
{
    /// <summary>
    /// 定义表的Column字段属性
    /// 现在支持设置字段的大小
    /// </summary>
    [System.AttributeUsage(AttributeTargets.All, Inherited = false, AllowMultiple = true)]
    public sealed class ColumnAttribute : Attribute
    {
        /// <summary>
        /// Column Attribute contstruction
        /// </summary>
        /// <param name="size"></param>
        public ColumnAttribute()
        {
        }

        /// <summary>
        /// 初始化Column Attribute，并且设置字段大小
        /// </summary>
        /// <param name="size">字段大小</param>
        public ColumnAttribute(int size)
        {
            Size = size;
        }



        /// <summary>
        /// 返回字段大小
        /// </summary>
        public int Size { get; }

        /// <summary>
        /// 返回字段类型
        /// </summary>
        public string TypeName { get; set; }

    }
}
