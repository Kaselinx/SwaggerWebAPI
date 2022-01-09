using System;

namespace SwaggerWebAPI.Model
{
    public class WeatherForecast
    {

        /// <summary>
        /// The date of the forecast in ISO-whatever format
        /// </summary>
        /// <example>2020-01-01</example>
        public DateTime Date { get; set; }

        /// <summary>
        /// Temperature in celcius
        /// </summary>
        /// <example>25</example>
        public int TemperatureC { get; set; }

        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);

        /// <summary>
        /// A textual summary
        /// </summary>
        /// <example>Cloudy with a chance of rain</example>
        public string Summary { get; set; }
    }
}
