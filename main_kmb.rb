#!/usr/bin/ruby

require "faraday"
require "sqlite3"
require "active_record"

module BusOpenData
  DOMAIN = "https://data.etabus.gov.hk"
  ROUTE = "v1/transport/kmb/route"
  ROUTE_STOP = "v1/transport/kmb/route-stop"
  STOP = "v1/transport/kmb/stop"
end

ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDOUT)

dbConnect = ActiveRecord::Base.establish_connection(
  :adapter => "sqlite3",
  :database => "bus_kmb.db",
)

routes_sql = <<-SQL
    CREATE TABLE IF NOT EXISTS routes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        co varchar(16), 
        route varchar(16),
        orig_en varchar(255),
        orig_tc varchar(255),
        dest_en varchar(255),
        dest_tc varchar(255),
        service_type varchar(16) NOT NULL
    );
SQL

route_stops_sql = <<-SQL
    CREATE TABLE IF NOT EXISTS route_stops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        co varchar(16), 
        route varchar(16),
        dir varchar(16),
        seq INTEGER,
        stop varchar(255),
        service_type varchar(16) NOT NULL
    );
SQL

stops_sql = <<-SQL
    CREATE TABLE IF NOT EXISTS stops (
        stop varchar(255) PRIMARY KEY,
        name_tc varchar(255), 
        name_en varchar(255),
        lat double,
        long double
    );
SQL

ActiveRecord::Base.connection.execute(routes_sql)
ActiveRecord::Base.connection.execute(route_stops_sql)
ActiveRecord::Base.connection.execute(stops_sql)

class Route < ActiveRecord::Base
end

class RouteStop < ActiveRecord::Base
end

class Stop < ActiveRecord::Base
end

# download json
conn = Faraday.new(BusOpenData::DOMAIN) do |f|
  f.response :json
end

def save_bus_route(conn, path)
  STDOUT.puts "Fetching from : #{path}"
  response = conn.get(path)
  Route.transaction do
    response.body["data"].each { |r|
      Route.create(
        co: "KMB",
        route: r["route"],
        orig_en: r["orig_en"],
        orig_tc: r["orig_tc"],
        dest_en: r["dest_en"],
        dest_tc: r["dest_tc"],
        service_type: r["service_type"] || "-1",
      )
    }
  end
end

save_bus_route(conn, BusOpenData::ROUTE)

def save_route_stop(conn)
  path = "#{BusOpenData::ROUTE_STOP}"
  STDOUT.puts "Fetching #{BusOpenData::DOMAIN}#{path}"
  response = conn.get(path)
  RouteStop.transaction do
    response.body["data"].each { |r|
      RouteStop.new(
        co: "KMB",
        route: r["route"],
        dir: r["bound"],
        seq: r["seq"],
        stop: r["stop"],
        service_type: r["service_type"] || "-1",
      ).save
    }
  end
end

save_route_stop(conn)

def save_stop(conn)
  path = "#{BusOpenData::STOP}"
  STDOUT.puts "Fetching #{BusOpenData::DOMAIN}#{path}"
  response = conn.get(path)
  Stop.transaction do
    response.body["data"].each { |s|
      Stop.create(
        stop: s["stop"],
        name_tc: s["name_tc"],
        name_en: s["name_en"],
        lat: s["lat"].to_f,
        long: s["long"].to_f,
      )
    }
  end
end

save_stop(conn)
