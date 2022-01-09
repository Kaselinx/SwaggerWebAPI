using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SwaggerWebAPI.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace SwaggerWebAPI.Controllers
{
    [Route("api/[controller]/[Action]")]
    [ApiController]
    public class WebApiController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WebApiController> _logger;
        private DataAccess.DataAccessService _dbaccess;

        public WebApiController(ILogger<WebApiController> logger)
        {
            _logger = logger;
            _dbaccess = new DataAccess.DataAccessService("Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=TCPS;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=True"
                                                            , "Data Source=(localdb)\\ProjectsV13;Initial Catalog=TCPS2;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=True"
                                                            );
        }

        [HttpGet]
        public IEnumerable<WeatherForecast> APIGet()
        {
            _logger.LogInformation("APIGet Called!");
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55),
                Summary = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }




        /// <summary>
        /// test query3333
        /// </summary>
        ///// <returns></returns>
        [HttpPost]
        public IEnumerable<PXLoading> QueryLoading()
        {
            _logger.LogInformation("TransationSafe Called!");

            var result = _dbaccess.QueryAll<PXLoading>().Result;

            var data = result.ToList();

            return data;
        }

        /// <summary>
        /// test trans safty
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public bool TestWithTransInsert()
        {
            _logger.LogInformation("TestWithTransInsert Called!");
            var result = _dbaccess.TestInsertWithTranscation();

            return result;
        }


        /// <summary>
        /// test trans safty with 2 DB
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public bool TestWithTransWith2Con()
        {
            _logger.LogInformation("TestWithTransWith2Con Called!");
            var result = _dbaccess.TestInsertWithDiffDB();

            return result;
        }


        /// <summary>
        /// test trans safty with 2 server.. Failed Not support by dapper
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public bool TestWithTransWithMulti()
        {
            _logger.LogInformation("TestWithTransWith2Con Called!");
            var result = _dbaccess.TestInsertWithMultiServer();

            return result;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="forecast"></param>
        [HttpPost]
        public void Submit2(WeatherForecast forecast)
        {
            // blah
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="forecast"></param>
        [HttpPost]
        public void Submit(WeatherForecast forecast)
        {
            // blah
        }
    }
}
