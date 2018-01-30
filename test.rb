require "google_drive"
require "./confluence"

def build_create_table(ws, table_name)
  format = <<-EOS
create table %{table} (
%{rows}
  minute int,
  second int
)
partitioned by (year int, month int, day int, hour int)
stored as parquet
;
EOS

  row_format = "  %{name} %{type} comment \"%{comment}\","

  rows = (2..ws.num_rows).map { |r|
    row_format % { 
      name:    ws[r,1].ljust(31),
      type:    ws[r,2].ljust(20),
      comment: ws[r,3],
    }
  }.join("\n")
  
  return format % {table: table_name, rows: rows}
end

def build_insert(ws, table_name, raw_table_name)
  format = <<-EOS
insert into %{table}
partition (year = ${hiveconf:year}, month = ${hiveconf:month}, day = ${hiveconf:day}, hour = ${hiveconf:hour})
select 
  %{selects}
  from_unixtime(nginx_time_stamp, "m"), from_unixtime(nginx_time_stamp, "s")
from %{raw_table}
lateral view json_tuple(json_data,
  %{columns_dq}
) data as
  %{columns}
;
EOS

  columns_list = (2..ws.num_rows).map{|r|ws[r,3]}

  return format % {
    selects:    (2..ws.num_rows).map{|r|ws[r,4] + ","}.join,
    columns:    columns_list.join(",") + ",",
    columns_dq: columns_list.map{|c|"\"%s\"" % c}.join(",") + ",",
    table:      table_name,
    raw_table:  raw_table_name,
  }
end

def build_table_name(log_name)
  return log_name + '_logs'
end

def build_raw_table_name(log_name)
  product_name, log_type = log_name.split("_")
  return product_name + '_raw_' + log_type + '_logs'
end

def update_page(confluence, page_id, content)
  confluence.execute(a: 'storePage', id: page_id, content: "'#{content}'")
end

def fetch_metadata_list(spreadsheet)
  ws = spreadsheet.worksheet_by_title('confluence')
  (2..ws.num_rows).map { |r|
    {
      log_name: ws[r,1],
      sql_type: ws[r,2],
      page_id:  ws[r,3],
    }
  }
end

def update_page_by_metadata(confluence, spreadsheet, metadata)
  ws = spreadsheet.worksheet_by_title(metadata[:log_name])

  sql = case metadata[:sql_type]
  when 'create_table' then
    build_create_table(ws, build_table_name(metadata[:log_name]))
  when 'insert' then
    build_insert(ws, build_table_name(metadata[:log_name]), build_raw_table_name(metadata[:log_name]))
  end

  content = '{excerpt}{code:language=sql|theme=Django}%s{code}{excerpt}' % sql
  update_page(confluence, metadata[:page_id], content)
end

session = GoogleDrive::Session.from_config("google_drive_config.json")
spreadsheet = session.file_by_id('1FVu3LahWtaW2nFoyn61ooJY8bqpb60SFXOnJDjwCwOI')
confluence = Confluence.new
metadata_list = []
if ARGV.size == 0 then
  metadata_list = fetch_metadata_list(spreadsheet)
else
  metadata_list = fetch_metadata_list(spreadsheet).select{|m|ARGV.include?(m[:log_name])}
end
metadata_list.each{|m|update_page_by_metadata(confluence, spreadsheet, m)}

