using Dapper.Contrib.Extensions;
using System;

namespace SwaggerWebAPI.Model
{
    [Table("PXLoading")]
    public class PXLoading
    {
        [ExplicitKey]
        public int PXLoadingId { get; set; }
        public string PlanCode { get; set; }
        public int BandStage { get; set; }
        public int Limits { get; set; }
        public decimal BandValue { get; set; }
        public DateTime LastModifyTime { get; set; }

        public string LastModifyUser { get; set; }

    
    }
}
