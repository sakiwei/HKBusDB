#!/usr/bin/ruby

require "faraday"
require "sqlite3"
require "active_record"

module BusOpenData
  API_VERSION = "2"
  DOMAIN = "https://rt.data.gov.hk"
  CTB_ROUTE = "/v#{API_VERSION}/transport/citybus/route/CTB"
  ROUTE_STOP = "/v#{API_VERSION}/transport/citybus/route-stop/"
  STOP = "/v#{API_VERSION}/transport/citybus/stop/"
end

ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDOUT)

dbConnect = ActiveRecord::Base.establish_connection(
  :adapter => "sqlite3",
  :database => "bus_ctb.db",
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
        service_type varchar(16)
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
        service_type varchar(16)
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
        co: r["co"],
        route: r["route"],
        orig_en: r["orig_en"],
        orig_tc: r["orig_tc"],
        dest_en: r["dest_en"],
        dest_tc: r["dest_tc"],
      )
    }
  end
end

save_bus_route(conn, BusOpenData::CTB_ROUTE)

def save_route_stop(conn, co, route, bound, canRetry)
  path = "#{BusOpenData::ROUTE_STOP}#{co}/#{route}/#{bound}"
  STDOUT.puts "Fetching #{BusOpenData::DOMAIN}#{path}"
  response = conn.get(path)
  if response.status == 429 && canRetry
    sleep(0.3)
    save_route_stop(conn, co, route, bound, false)
  else
    RouteStop.transaction do
      response.body["data"].each { |r|
        RouteStop.new(
          co: r["co"],
          route: r["route"],
          dir: r["dir"],
          seq: r["seq"],
          stop: r["stop"],
        ).save
      }
    end
  end
end

# save_route_stop(conn, "CTB", "118", "outbound")
puts "Total routes: #{Route.all.length}"

all_routes = Route.all
all_routes.each do |r|
  STDOUT.puts "Start => #{r["co"]}-#{r["route"]}"
  save_route_stop(conn, r["co"], r["route"], "inbound", true)
  sleep(0.1)
  save_route_stop(conn, r["co"], r["route"], "outbound", true)
  STDOUT.puts "<== End"
  sleep(0.1)
end

def save_stop(conn, stop_id, canRetry)
  path = "#{BusOpenData::STOP}#{stop_id}"
  STDOUT.puts "Fetching #{BusOpenData::DOMAIN}#{path}"
  response = conn.get(path)
  if response.status == 429 && canRetry
    save_stop(conn, stop_id, false)
  else
    s = response.body["data"]
    Stop.new(
      stop: s["stop"],
      name_tc: s["name_tc"],
      name_en: s["name_en"],
      lat: s["lat"].to_f,
      long: s["long"].to_f,
    ).save
  end
end

stop_list = RouteStop.select(:stop).distinct
puts stop_list.length
stop_list.each { |s|
  stop_id = s["stop"]
  save_stop(conn, stop_id, true)
  sleep(0.1)
}
