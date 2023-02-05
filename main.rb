#!/usr/bin/ruby
require "sqlite3"
require "faraday"
require "zip"
require "digest"

module BusOpenData
  KMB_DB = "bus_kmb.db"
  NWFB_CTB_DB = "bus_nwfb_ctb.db"
  OUTPUT_DB = "bus.db"
end

routes_sql = <<-SQL
    CREATE TABLE IF NOT EXISTS routes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        co varchar(16), 
        route varchar(16),
        orig_en varchar(255),
        orig_tc varchar(255),
        dest_en varchar(255),
        dest_tc varchar(255),
        service_type varchar(16),
        circular varchar(16)
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

# Open a database
new_db = SQLite3::Database.new(BusOpenData::OUTPUT_DB)
new_db.results_as_hash = true

new_db.execute(routes_sql)
new_db.execute(route_stops_sql)
new_db.execute(stops_sql)

nwfb_ctb_db = SQLite3::Database.new(BusOpenData::NWFB_CTB_DB)
nwfb_ctb_db.results_as_hash = true

kmb_db = SQLite3::Database.new(BusOpenData::KMB_DB)
kmb_db.results_as_hash = true

def standise_name(name)
  name.gsub(/\s{2,10}/, " ").gsub("（", "(").gsub("）", ")")
  return name
end

nwfb_ctb_db.execute("select * from routes").each { |r|
  def exception_case(name, route_name)
    if name == "TAI PO CENTRE"
      return "TAI PO CENTRAL"
    elsif name == "KENNEDY TOWN / CENTRAL"
      return "KENNEDY TOWN"
    elsif name == "白田邨"
      return "深水埗(白田邨)"
    elsif name == "堅尼地城／中環"
      return "堅尼地城"
    elsif name == "PAK TIN ESTATE"
      return "SHAM SHUI PO (PAK TIN ESTATE)"
    elsif name == "SO UK ESTATE"
      return "CHEUNG SHA WAN (SO UK ESTATE)"
    elsif name == "蘇屋邨"
      return "長沙灣(蘇屋邨)"
    elsif name == "TSING YI (CHEUNG ON)"
      return "TSING YI (CHEUNG ON ESTATE)"
    elsif name == "青衣(長安)"
      return "青衣(長安邨)"
    elsif name == "灣仔(會展新翼)"
      return "灣仔(會展)"
    elsif route_name == "191R" && name == "LOHAS PARK"
      return "LOHAS PARK STATION"
    elsif route_name == "191R" && name == "日出康城"
      return "康城站"
    elsif route_name == "680" && name == "ADMIRALTY (EAST)"
      return "ADMIRALTY STATION (EASTREET)"
    elsif route_name == "680" && name == "金鐘(東)"
      return "金鐘站(東)"
    elsif route_name == "307" && name == "CENTRAL (FERRY PIERS) / SHEUNG WAN"
      return "CENTRAL (CENTRAL FERRY PIERS)"
    elsif route_name == "N307" && name == "SHEUNG WAN"
      return "CENTRAL (MACAU FERRY)"
    elsif route_name == "N307" && name == "上環"
      return "中環(港澳碼頭)"
    elsif route_name == "103" && name == "CHUK YUEN"
      return "CHUK YUEN ESTATE"
    elsif route_name == "103" && name == "竹園"
      return "竹園邨"
    elsif route_name == "907C" && name == "WAN CHAI (HKCEC)"
      return "WAN CHAI (HKCEC EXTENSION)"
    elsif route_name == "907C" && name == "灣仔(會議展覽中心)"
      return "灣仔(會展)"
    elsif route_name == "907B" && name == "WAN CHAI (HKCEC)"
      return "WAN CHAI (HKCEC EXTENSION)"
    elsif route_name == "907B" && name == "灣仔(會議展覽中心)"
      return "灣仔(會展)"
    end
    return name
  end

  begin
    route_name = r["route"].upcase
    orig_tc = r["orig_tc"]
    dest_tc = r["dest_tc"]
    is_circular = orig_tc.include?("循環線") || dest_tc.include?("循環線")
    new_db.execute("insert into routes (co, route, orig_en, orig_tc, dest_en, dest_tc, service_type, circular) values (?, ?, ?, ?, ?, ?, ?, ?)",
                   r["co"].upcase,
                   route_name,
                   exception_case(standise_name(r["orig_en"].upcase), route_name),
                   exception_case(standise_name(r["orig_tc"]).gsub(/\s/, ""), route_name),
                   exception_case(standise_name(r["dest_en"].upcase), route_name),
                   exception_case(standise_name(r["dest_tc"]).gsub(/\s/, ""), route_name),
                   "1",
                   is_circular ? "Y" : "N")
  rescue => exception
    STDOUT.puts exception
  end
}

kmb_db.execute("select * from routes").each { |r|
  def exception_case(name, route_name)
    if name == "TSIM SHA TSUI EAST (MODY ROAD)"
      return "TSIM SHA TSUI (MODY ROAD)"
    elsif name == "尖沙咀東(麼地道)"
      return "尖沙咀(麼地道)"
    elsif name == "TSIM SHA TSUI (CIRCULAR)"
      return "TSIM SHA TSUI (MODY ROAD)"
    elsif name == "尖沙咀(循環線)"
      return "尖沙咀(麼地道)"
    elsif name.end_with?("ST)")
      return name.sub("ST)", "STREET)")
    elsif name.include?("SHATIN")
      return name.gsub("SHATIN", "SHA TIN")
    elsif route_name == "109" && name == "HO MAN TIN"
      return "HO MAN TIN ESTATE"
    elsif route_name == "109" && name == "何文田"
      return "何文田邨"
    elsif name == "PING SHEK / CHOI HUNG STATION"
      return "PING SHEK ESTATE"
    elsif name == "坪石/彩虹站"
      return "坪石邨"
    elsif name == "KOWLOON CITY FERRY"
      return "KOWLOON CITY FERRY PIER"
    elsif name == "WAN CHAI(HKCEC)" || name == "WAN CHAI (HKCEC)"
      return "WAN CHAI (HKCEC EXTENSION)"
    elsif route_name == "680" && name == "LEE ON"
      return "MA ON SHAN (LEE ON ESTATE)"
    elsif route_name == "680" && name == "利安"
      return "馬鞍山(利安邨)"
    elsif route_name == "307P" && name == "CAUSEWAY BAY (TIN HAU)"
      return "TIN HAU STATION"
    elsif route_name == "307P" && name == "銅鑼灣(天后)"
      return "天后站"
    elsif route_name == "N307" && name == "SHEUNG WAN"
      return "CENTRAL (MACAU FERRY)"
    elsif route_name == "N307" && name == "上環"
      return "中環(港澳碼頭)"
    elsif name.end_with?(" RD")
      return name.sub(" RD", " ROAD")
    elsif route_name == "107" && name == "WAH KWAI"
      return "WAH KWAI ESTATE"
    elsif route_name == "107" && name == "華貴"
      return "華貴邨"
    elsif route_name == "182" && name == "YU CHUI COURT"
      return "SHA TIN (YU CHUI COURT)"
    elsif route_name == "182" && name == "愉翠苑"
      return "沙田(愉翠苑)"
    elsif route_name == "619" && name == "SHUN LEE"
      return "SHUN LEE ESTATE"
    elsif route_name == "619" && name == "順利"
      return "順利邨"
    elsif route_name == "690" && name == "HONG SING GARDEN"
      return "TSEUNG KWAN O (HONG SING GARDEN)"
    elsif route_name == "690" && name == "康盛花園"
      return "將軍澳(康盛花園)"
    elsif route_name == "907C" && name == "WAN CHAI (CONVENTION CENTRE)"
      return "WAN CHAI (HKCEC EXTENSION)"
    elsif route_name == "907B" && name == "WAN CHAI (CONVENTION CENTRE)"
      return "WAN CHAI (HKCEC EXTENSION)"
    elsif route_name == "907B" && name == "TAI PO KWONG FUK PLAYGROUND"
      return "TAI PO (KWONG FUK FOOTBALL GROUND)"
    elsif route_name == "907B" && name == "大埔廣福球場"
      return "大埔(廣福球場)"
    elsif route_name == "914" && name == "CAUSEWAY BAY (TIN HAU)"
      return "TIN HAU STATION"
    elsif route_name == "914" && name == "銅鑼灣(天后)"
      return "天后站"
    elsif route_name == "936" && name == "SHEK WAI KOK"
      return "TSUEN WAN (SHEK WAI KOK)"
    elsif route_name == "936" && name == "石圍角"
      return "荃灣(石圍角)"
    elsif route_name == "671" && name == "AP LEI CHAU LEE LOK ST"
      return "AP LEI CHAU (LEE LOK STREET)"
    end
    return name
  end

  begin
    route_name = r["route"].upcase
    orig_tc = r["orig_tc"]
    dest_tc = r["dest_tc"]
    is_circular = orig_tc.include?("循環線") || dest_tc.include?("循環線")
    new_db.execute("insert into routes (co, route, orig_en, orig_tc, dest_en, dest_tc, service_type, circular) values (?, ?, ?, ?, ?, ?, ?, ?)",
                   r["co"].upcase,
                   route_name,
                   exception_case(standise_name(r["orig_en"].upcase), route_name),
                   exception_case(standise_name(r["orig_tc"]).gsub(/\s/, ""), route_name),
                   exception_case(standise_name(r["dest_en"].upcase), route_name),
                   exception_case(standise_name(r["dest_tc"]).gsub(/\s/, ""), route_name),
                   r["service_type"],
                   is_circular ? "Y" : "N")
  rescue => exception
    STDOUT.puts exception
  end
}

distinct_routes = new_db.execute("select DISTINCT route from routes")

exclude_routes = ["148R", "191R", "302", "307A", "907C", "907B"]
removal_id = []
distinct_routes.each { |route_name|
  routes = new_db.execute("select * from routes where route = ? and co = 'KMB'", route_name["route"])
  route_dest_orig_set = []
  if routes.length >= 1
    routes.each { |r|
      if !route_dest_orig_set.include?("#{r["route"]}|#{r["dest_en"]}|#{r["orig_en"]}|#{r["service_type"]}") && r["circular"] == "N" && !exclude_routes.include?(r["route"])
        add_a = "#{r["route"]}|#{r["dest_en"]}|#{r["orig_en"]}|#{r["service_type"]}"
        add_b = "#{r["route"]}|#{r["orig_en"]}|#{r["dest_en"]}|#{r["service_type"]}"
        route_dest_orig_set << add_a
        route_dest_orig_set << add_b
      else
        removal_id << r["id"]
      end
    }
  end
}

puts removal_id.join(",")

merge_items = []
removal_id.each { |route_id|
  routes = new_db.execute("select * from routes where id = ?", route_id)
  routes.each { |r|
    merge_target_a = new_db.execute("select * from routes where route = ? and orig_en = ? and dest_en = ? and co in ('NWFB', 'CTB') LIMIT 1", r["route"], r["orig_en"], r["dest_en"])
    merge_target_b = new_db.execute("select * from routes where route = ? and orig_en = ? and dest_en = ? and co in ('NWFB', 'CTB') LIMIT 1", r["route"], r["dest_en"], r["orig_en"])
    if !merge_target_a.empty?
      # remove a
      merge_items << "#{merge_target_a[0]["id"]},#{merge_target_a[0]["co"]}"
    end
    if !merge_target_b.empty?
      # remove b
      remove_b_key = "#{merge_target_b[0]["id"]},#{merge_target_b[0]["co"]}"
      if !merge_items.include?(remove_b_key)
        merge_items << remove_b_key
      end
    end
    # remove route_id
    if !exclude_routes.include?(r["route"])
      new_db.execute("delete from routes where id = ? and circular = 'N'", route_id)
    end
  }
}

# puts merge_items.join(",")
merge_items.each { |merge_item|
  route_comp = merge_item.split(",", -1)
  route_id = route_comp[0]
  new_co = route_comp[1] + "+KMB"
  routes = new_db.execute("select * from routes where id = ?", route_id)
  routes.each { |r|
    merge_target_a = new_db.execute("select * from routes where route = ? and (orig_en = ? or dest_en = ?) and co = 'KMB' LIMIT 1", r["route"], r["orig_en"], r["dest_en"])
    merge_target_b = new_db.execute("select * from routes where route = ? and (orig_en = ? or dest_en = ?) and co = 'KMB' LIMIT 1", r["route"], r["dest_en"], r["orig_en"])
    if !merge_target_a.empty?
      # update table
      new_db.execute("update routes set co = ? where id = ?", new_co, merge_target_a[0]["id"])
    end
    if !merge_target_b.empty?
      # update table
      new_db.execute("update routes set co = ? where id = ?", new_co, merge_target_b[0]["id"])
    end
    # remove route_id
    new_db.execute("delete from routes where id = ?", route_id)
  }
}

new_db.execute("select * from routes where co like 'NWFB+%'").each { |r|
  # new_db.execute("select * from routes where route like '%#{r["route"]}%' and ((orig_en = ? or dest_en = ?) or (orig_en = ? or dest_en = ?)) and co in ('NWFB', 'KMB', 'CTB')", r["orig_en"], r["orig_en"], r["dest_en"], r["dest_en"]).each { |r2|
  new_db.execute("select * from routes where route like '%#{r["route"]}%' and co in ('NWFB', 'KMB', 'CTB')").each { |r2|
    if !r2["route"].start_with?("X")
      new_db.execute("update routes set co = ? where id = ?", "NWFB+KMB", r2["id"])
    end
  }
}

new_db.execute("select * from routes where co like 'CTB+%'").each { |r|
  # new_db.execute("select * from routes where route like '%#{r["route"]}%' and ((orig_en = ? or dest_en = ?) or (orig_en = ? or dest_en = ?)) and co in ('NWFB', 'KMB', 'CTB')", r["orig_en"], r["orig_en"], r["dest_en"], r["dest_en"]).each { |r2|
  new_db.execute("select * from routes where route like '%#{r["route"]}%' and co in ('NWFB', 'KMB', 'CTB')").each { |r2|
    if !r2["route"].start_with?("X")
      new_db.execute("update routes set co = ? where id = ?", "CTB+KMB", r2["id"])
    end
  }
}

nwfb_ctb_db.execute("select * from route_stops").each { |r|
  begin
    new_db.execute("insert into route_stops (co, route, dir, seq, stop) values (?, ?, ?, ?, ?)",
                   r["co"].upcase,
                   r["route"].upcase,
                   r["dir"].upcase,
                   r["seq"],
                   r["stop"])
  rescue => exception
    STDOUT.puts exception
  end
}

kmb_db.execute("select * from route_stops").each { |r|
  begin
    new_db.execute("insert into route_stops (co, route, dir, seq, stop, service_type) values (?, ?, ?, ?, ?, ?)",
                   r["co"].upcase,
                   r["route"].upcase,
                   r["dir"].upcase,
                   r["seq"],
                   r["stop"],
                   r["service_type"])
  rescue => exception
    STDOUT.puts exception
  end
}

nwfb_ctb_db.execute("select * from stops").each { |r|
  begin
    new_db.execute("insert into stops (stop, name_tc, name_en, lat, long) values (?, ?, ?, ?, ?)",
                   r["stop"],
                   r["name_tc"],
                   r["name_en"],
                   r["lat"],
                   r["long"])
  rescue => exception
    STDOUT.puts exception
  end
}

kmb_db.execute("select * from stops").each { |r|
  begin
    new_db.execute("insert into stops (stop, name_tc, name_en, lat, long) values (?, ?, ?, ?, ?)",
                   r["stop"],
                   r["name_tc"],
                   r["name_en"],
                   r["lat"],
                   r["long"])
  rescue => exception
    STDOUT.puts exception
  end
}

def generate_bus_db_zip
  zip_file = Tempfile.new("bus_db.zip")

  begin
    file_names = ["bus.db"]
    Zip::File.open(zip_file.path, Zip::File::CREATE) do |zipfile|
      file_names.each do |filename|
        zipfile.add(filename, filename)
      end
    end

    puts Digest::MD5.hexdigest(File.read("bus.db"))

    zip_data = File.read(zip_file.path)
    File.open("bus_db.zip", "w") do |f|
      f.write(zip_data)
    end
  ensure
    zip_file.close
    zip_file.unlink
  end
  
end

generate_bus_db_zip()