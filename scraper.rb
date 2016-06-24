require 'scraperwiki'
require 'csv'

ENDPOINT = 'https://buyandsell.gc.ca/procurement-data/csv/tender/active'

def clean_table
  ScraperWiki.sqliteexecute('DELETE FROM data')
rescue SqliteMagic::NoSuchTable
  puts "Data table does not exist yet"
end

def fetch_results
  raw = strip_bom(open(ENDPOINT, 'r:utf-8').read)
  doc = CSV.parse(raw, headers: true, encoding: 'UTF-8')
  doc.map { |entry| process_entry entry.to_h }.each do |lead|
    ScraperWiki.save_sqlite(%w(language reference_number), lead)
  end
end

def strip_bom(text)
  text.sub(/^\xef\xbb\xbf/, '')
end

def process_entry(lead)
  lead['gsin'] &&= split_industries(lead['gsin'])
  lead
end

def split_industries(gsin)
  regex_split = gsin.split(/, ([0-9A-Z]+ -)/)
  segments = [regex_split.shift]
  industries = segments.push regex_split.each_slice(2).map(&:join)
  industries.flatten.join(' | ')
rescue
  gsin
end

clean_table
fetch_results
