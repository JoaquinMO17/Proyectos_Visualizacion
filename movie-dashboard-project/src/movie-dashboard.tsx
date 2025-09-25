import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell, Area, AreaChart } from 'recharts';
import { TrendingUp, TrendingDown, Film, Users, Star, Globe, Calendar, Award, Clock, Database, BarChart3 } from 'lucide-react';

const Dashboard = () => {
  const [movieStats, setMovieStats] = useState(null);
  const [distributionData, setDistributionData] = useState([]);
  const [themesData, setThemesData] = useState([]);
  const [companiesData, setCompaniesData] = useState([]);
  const [countriesData, setCountriesData] = useState([]);
  const [directorsData, setDirectorsData] = useState([]);
  const [ratingDistribution, setRatingDistribution] = useState([]);
  const [topRated, setTopRated] = useState([]);
  const [ratingTrends, setRatingTrends] = useState([]);
  const [mongoStats, setMongoStats] = useState(null);
  const [mongoAggregations, setMongoAggregations] = useState(null);
  const [mongoMovies, setMongoMovies] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [
          statsRes,
          distRes,
          themesRes,
          companiesRes,
          countriesRes,
          directorsRes
        ] = await Promise.all([
          fetch("/api/movies/distribution-by-year?group_by=decade"), 
          fetch("/api/movies/themes-by-decade"), 
          fetch("/api/production/countries"), 
          fetch("/api/production/directors"), 
          fetch("/api/ratings/distribution"), 
          fetch("/api/ratings/top-rated"), 

        ]);

        const [
          statsData,
          distData,
          themesData,
          companiesData,
          countriesData,
          directorsData

        ] = await Promise.all([
          statsRes.json(),
          distRes.json(),
          themesRes.json(),
          companiesRes.json(),
          countriesRes.json(),
          directorsRes.json()
        ]);

        // Movies API
        setMovieStats(statsData);
        setDistributionData(distData.data);
        setThemesData(
          Array.isArray(themesData.data)
            ? themesData.data
            : Object.entries(themesData.data).map(([theme, count]) => ({ theme, count }))
        );
        setCompaniesData(companiesData.data);
        setCountriesData(countriesData.data);
        setDirectorsData(directorsData.data);



        setLoading(false);
      } catch (error) {
        console.error("Error fetching dashboard data:", error);
      }
    };

    fetchData();
  }, []);
    // ... el resto del código del componente sigue aquí
    const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d', '#ffc658', '#d0ed57'];
  const RADIAN = Math.PI / 180;
  const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index }) => {
    const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);
    return (
      <text x={x} y={y} fill="white" textAnchor={x > cx ? 'start' : 'end'} dominantBaseline="central">
        {`${(percent * 100).toFixed(0)}%`}
      </text>
    );
  };

  const renderActiveShape = (props) => {
    const { cx, cy, innerRadius, outerRadius, startAngle, endAngle, fill, payload } = props;
    return (
      <g>
        <text x={cx} y={cy} dy={8} textAnchor="middle" fill={fill}>
          {payload.name}
        </text>
        <sector cx={cx} cy={cy} innerRadius={innerRadius} outerRadius={outerRadius + 5} startAngle={startAngle} endAngle={endAngle} fill={fill} />
      </g>
    );
  };

  const StatCard = ({ title, value, icon, description }) => {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md flex items-center space-x-4">
        <div className="p-3 rounded-full bg-blue-100 text-blue-500">
          {icon}
        </div>
        <div>
          <h3 className="text-gray-500 text-sm font-medium">{title}</h3>
          <p className="text-2xl font-semibold text-gray-900">{value}</p>
          <p className="text-gray-400 text-xs mt-1">{description}</p>
        </div>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-screen bg-gray-100">
        <div className="text-lg font-semibold text-gray-700">Cargando dashboard...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 p-8">
      <header className="mb-8">
        <h1 className="text-4xl font-bold text-gray-900">Movie Dashboard</h1>
        <p className="mt-2 text-lg text-gray-600">Análisis de datos de películas con FastAPI, PostgreSQL y MongoDB</p>
      </header>
      
      <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard title="Total de Películas" value={movieStats?.total_movies || 'N/A'} icon={<Film className="h-6 w-6" />} description="Cargado desde PostgreSQL" />
        <StatCard title="Promedio de Votos" value={movieStats?.avg_votes ? `${(movieStats.avg_votes / 1000).toFixed(1)}k` : 'N/A'} icon={<Users className="h-6 w-6" />} description="Votos promedio por película" />
        <StatCard title="Rating Promedio" value={movieStats?.avg_rating?.toFixed(1) || 'N/A'} icon={<Star className="h-6 w-6" />} description="Rating general de películas" />
        <StatCard title="Tiempo Promedio" value={movieStats?.avg_duration ? `${Math.round(movieStats.avg_duration)} min` : 'N/A'} icon={<Clock className="h-6 w-6" />} description="Duración promedio de películas" />
      </section>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <section className="bg-white p-6 rounded-lg shadow-md">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Distribución de Películas por Década</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={distributionData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="decade" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </section>

        <section className="bg-white p-6 rounded-lg shadow-md">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Tendencias de Rating a lo Largo del Tiempo</h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={ratingTrends}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="average_rating" stroke="#10b981" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </section>
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
        <section className="bg-white p-6 rounded-lg shadow-md">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Temas de Películas por Década</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={themesData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="theme" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill="#6366f1" />
            </BarChart>
          </ResponsiveContainer>
        </section>

        <section className="bg-white p-6 rounded-lg shadow-md">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Distribución de Ratings</h2>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={ratingDistribution}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="rating" />
              <YAxis />
              <Tooltip />
              <Area type="monotone" dataKey="count" stroke="#f59e0b" fillOpacity={1} fill="url(#colorUv)" />
            </AreaChart>
          </ResponsiveContainer>
        </section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
        <section className="bg-white p-6 rounded-lg shadow-md overflow-x-auto">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Top 10 Directores</h2>
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Director</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Películas</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {directorsData.map((director, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{director.director_name}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{director.movie_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
        
        <section className="bg-white p-6 rounded-lg shadow-md overflow-x-auto">
          <h2 className="text-2xl font-semibold text-gray-800 mb-4">Top 10 Películas Mejor Calificadas</h2>
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">#</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Película</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rating</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Votos</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {topRated.map((movie, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    #{index + 1}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{movie.title}</td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <Star className="h-4 w-4 text-yellow-400 mr-1" />
                      <span className="text-sm font-semibold text-gray-900">{movie.rating}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {(movie.votes / 1000000).toFixed(1)}M votos
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
};

export default Dashboard;