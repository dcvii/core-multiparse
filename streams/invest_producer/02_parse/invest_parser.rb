#
# Author:: Michael Bowen (<michael.bowen@full360.com>)
# Copyright:: Copyright (c) 2014 Full 360 Inc.
# All rights reserved
#
############################################################
#       File:       IParse_producer.rb
#       Purpose:    general parser
#
#       Version:
#       Parameters:
#       Created:
#       Author:   mbowen
#       Parameters:
#       User to run with: 
#
#	modified 20140906.1400 by mbowen
#
############################################################

#require 'pry'


module XOB
	module IParse
		class ParseRuleSet

			HERE = "#{$stream_dir}/02_parse/"

			attr_accessor :label

			def initialize(set_label)
				fspec = HERE+"#{set_label}.yml"
				item_list = get_filters(set_label)
				if item_list == nil then
					# init
					@label = set_label
					@rule_count = 0
					@rules = []
					import(set_label)
					persist
				else 
					@rules = item_list[:rules]
					@rule_count = item_list[:rule_count]
					@label = item_list[:label]
				end			
			end

			def persist
				fspec = HERE+"#{@label}.yml"
				item_list = {label:@label, rule_count:@rule_count, rules:@rules}
   			open(fspec, 'w') { |f|	f.puts YAML::dump(item_list)}
   	  end

   	  def label
   	  	return @label

   	  end

   	  def rules
   	  	return @rules
   	  	
   	  end

   	  def inc_freq(rule_key)
   	  	r = @rules.find_index {|h| h[:key] == rule_key}
   	  	@rules[r][:freq] +=1
   	  end

   	  def add(rule)
   	  	@rules << rule
   	  	@rule_count = @rules.length
   	  end

  		def get_filters(label)
   	  	fspec = "HERE#{label}.yml"
   	  	item_list = nil
  			item_list = YAML.load(File.read(fspec)) if File.exists?(fspec)
  		return item_list
  		end

			def report_freq
				f = File.open("#{$rough}/#{@label}_freq_report.txt",'w')
				#logger.info "== Frequency Analysis Report =="
				f << "-- #{@label} Frequency Report of #{Time.now.to_s} --\n"
				@rules.each do |h|
					line = "#{h[:template]}\t #{h[:line_category]}\t #{h[:line_type]}\t #{h[:freq]}\n"
					#logger.info line 
					f << line 
				end
				f.close
			end

			def sort_by_freq
				for i in 0..@rules.length-1
				  j = i
				  while j > 0 and @rules[j-1][:freq] > @rules[j][:freq]
				    tmp = @rules[j]
				    @rules[j] = @rules[j-1]
				    @rules[j-1] = tmp
				    j = j-1
				  end
				end				
				@rules.reverse!		
			end

  		def import(label)
				logger.info "-- initializing filter set #{label} --"

				fspec = "#{HERE}#{label}.rule"

				infile = open(fspec,'r')
				infile.each do |line|
			 		h = {}
			 		patt = /(C|H|A|R|V|P|M|E|S)(\d{1})\s+(DATA|HEADER|BLANK)\s+(type\d{3})\s+(.*)/.match(line) 
			 		if not /(^\W+$)/.match(line) then  #tolerates blank lines in the filter file
				 		if patt == nil then 
				 			logger.info line
				 			raise 'BAD PATTERN IN FILTER.DAT!'

				 		else
				 			h[:template] = patt[1] + patt[2]
				 			h[:line_category] = patt[3]
				 			h[:line_type] = patt[4]
				 			h[:regex] = Regexp.new(patt[5])
				 			h[:label] = nil
				 			h[:freq] = 0
				 			h[:key] = "#{patt[1]}#{patt[2]}_#{patt[3][0]}_#{patt[4]}"
				 			add(h)

				 		end			
				 	end	 		
				end	
			end
		end

		class Parser

			def initialize 
				@freq = []
				logger.info "== IParse PARSER =="
			end

			def catch_nil(obj)
				begin
					return obj.to_s
				rescue 
					return '' 
				end
			end

			def persist_it(h)
				fspec = $rough+"/#{File.basename(h[:file_name],'.txt')}.yml"
   			open(fspec, 'w') { |f|	f.puts YAML::dump(h)}
   	  end

			def fsize(fspec)
	      fname = File.basename(fspec)
	      act_fs = File.stat(fspec).size
	      fs = (act_fs/1024).to_i
	      if fs > 1024 then
	        fs = (fs/1024).to_i
	        logger.info "#{fname} is #{fs.to_s} MB"
	        x = "#{fs.to_s} MiB"  
	      else
	        logger.info "#{fname} is #{fs.to_s} KB"
	        x = "#{fs.to_s} KiB"
	      end
	      return x
	    end
		

			def clean_input_files

				shortlist = Dir.glob("#{$inbox}/*.txt")
				shortlist.each do |fspec|
					line_buffer = []

				 	t = Time.now
				 	infile = open(fspec,'r')
				 	infile.each do |line|
				 		cline = line.encode("UTF-8", :invalid => :replace, :undef => :replace, :replace => "?") #utf scrubber 		
				 		line_buffer << cline.gsub(/\x1A/,"\n")
				 	end
				 	infile.close

				 	outfile = open(fspec, 'w')
				 	line_buffer.each do |cleaned_line|
				 		outfile << cleaned_line
				 	end
				 	outfile.close
			 

				 	logger.info "#{File.basename(fspec)} cleaned - fix time: #{Time.now - t} seconds"
				 
			 	end
			end

			
			def cleanup(mess)

				Dir.glob(mess).each {|f| File.delete(f)}
			end

			def file_to_hash(fspec)
				h = {}
				h[:file_array] = []
				infile = File.open(fspec, 'r')
				h[:file_name] = File.basename(fspec)
				h[:file_size] = fsize(fspec)
				
				infile.each_line do |line| 
					h[:file_array] << line
				end

				h[:line_count] = h[:file_array].count
				h[:rough_line] = []
				h[:fine_line] = []

				return h
			end

			def archive_data

				etl_stream do

					# data will be archived with the data date name in buckets named after the production run date

					fspec = Dir.glob("#{$outbox}/*").first
					data_date = File.basename(fspec)[0..6]
					
					year = data_date[0..3]
					yearmo = data_date[0..6]
					s3_path = "#{year}/#{yearmo}"
					
					## now move the files to s3
					access_key_id 'xxxxxx'
					secret_access_key 'xxxxxx/WlRCSF'	

					aws_endpoint ='s3-us-west-2.amazonaws.com'
					s3_bucket='XOB.third-base/IParse/'+s3_path
					
					outbox_glob = $outbox+'/*'

					r=s3_put_multi_file "#{outbox_glob}" do			
						prefix_key_with_dir false				
						s3_endpoint aws_endpoint
						bucket_name s3_bucket
						#encryption_key_file  $s3_keyfile
					end


				end
			end



			def vbar(a)
			  # delimits an array
			  outline = ""
			  d='|'	 
		    if a.length >= 1 then
		      outline += a[0].to_s
		       a[1..a.length].each do |item|
		        outline += d+item.to_s

		      end 
		    end
			  return outline
			end


			def tight_vbar(a)
			  # delimits an array 
			  # like vbar but strips spaces
			  outline = ""
			  d='|'	 
		    if a.length >= 1 then
		      outline += a[0].to_s.strip
		       a[1..a.length].each do |item|
		        outline += d+item.to_s.strip

		      end 
		      outline += "\c\n"
		    end
			  return outline
			end

			def spl_rough_parse

				logger.info '-- SPL ROUGH PARSE --'
				clean_input_files

				file_set =[{file_name:'VS_A200', template_type:'A2'},
									{file_name:'VS_M020M', template_type:'M1'},{file_name:'VS_M020P', template_type:'M5'},
									{file_name:'VS_M030-1', template_type:'M2'}, {file_name:'VS_M030-2', template_type:'M3'},
									{file_name:'VS_M030', template_type:'M4'}, {file_name:'VS_P010', template_type:'P2'},
									{file_name:'VS_R010', template_type:'R1'}, {file_name:'VS_S010', template_type:'S1'}]			

		
				file_set.each do |set|
					prs = ParseRuleSet.new(set[:template_type])
					t = Time.now
					shortlist = Dir.glob("#{$inbox}/*#{set[:file_name]}.txt")
					shortlist.each do |fspec|

						fname = File.basename(fspec)
						outfile = File.open("#{$rough}/rough_#{fname}",'w')
						outlog = File.open("#{$rough}/log_#{fname}",'w')
						hfile = file_to_hash(fspec)

						hfile[:file_array].each do |line|

							h = test_line(line, prs.rules)
							if h == nil then
								#outfile << "NOMATCH\n"
								outlog << "-- no match --\n"
								
							else 
								# frequency analysis, bumps the counter on this filter (slow, needs to search the
								# filterset)
								prs.inc_freq(h[:key])

								case h[:line_category] 
								when 'DATA'
									outfile << "D#{h[:line_type].upcase} #{line}"
									hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
									outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"

								when 'HEADER'
									outfile << line
									hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
									outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"
									
								when 'BLANK'
									#outfile << "BLANK\n"
									outlog << "BLANK\n"
								end
							end
						end
					
						### PERSIST THE RULE SETS

						logger.info "#{set[:file_name]} parse time: #{Time.now - t} seconds"
						prs.sort_by_freq
						prs.persist
						prs.report_freq

						
						### FINE PARSE ROUTINES

						case set[:template_type]
						when 'A2' then
							parse_A2(hfile)

						when 'M1' then 
							parse_M1(hfile)

						when 'M2' then 
							parse_M2(hfile)

						when 'M3' then 
							parse_M3(hfile)

						when 'M4' then 
							parse_M4(hfile)	

						when 'M5' then
							parse_M5(hfile)

						when 'P2' then 
							parse_P2(hfile)

						when 'R1' then 
							parse_R1(hfile)

						when 'S1' then 
							parse_S1(hfile)

						else
							raise 'Hell'
						end

					end
				end
			end

			def vs_rough_parse

				logger.info '-- VS ROUGH PARSE --'
				clean_input_files

				file_set =[{file_name:'VS_A', template_type:'V1'},
									{file_name:'VS_B', template_type:'V2'}]			

				file_set.each do |set|
					prs = ParseRuleSet.new(set[:template_type])
					t = Time.now
					shortlist = Dir.glob("#{$inbox}/*#{set[:file_name]}.txt")
					shortlist.each do |fspec|

						fname = File.basename(fspec)
						outfile = File.open("#{$rough}/rough_#{fname}",'w')
						outlog = File.open("#{$rough}/log_#{fname}",'w')
						hfile = file_to_hash(fspec)

						hfile[:file_array].each do |line|

							h = test_line(line, prs.rules)
							if h == nil then
								#outfile << "NOMATCH\n"
								outlog << "-- no match --\n"
								
							else 
								# frequency analysis, bumps the counter on this filter (slow, needs to search the
								# filterset)
								prs.inc_freq(h[:key])

								case h[:line_category] 
								when 'DATA'
									outfile << "D#{h[:line_type].upcase} #{line}"
									hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
									outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"

								when 'HEADER'
									outfile << line
									hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
									outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"
									
								when 'BLANK'
									#outfile << "BLANK\n"
									outlog << "BLANK\n"
								end
							end
						end
					
						### PERSIST THE RULE SETS

						logger.info "#{set[:file_name]} parse time: #{Time.now - t} seconds"
						prs.sort_by_freq
						prs.persist
						prs.report_freq

						
						### FINE PARSE ROUTINES

						case set[:template_type]
						when 'V1' then
							parse_V1(hfile)

						when 'V2' then 
							parse_V2(hfile)

						else
							raise 'Hell'
						end

					end
				end
			end

			def ss_rough_parse

				logger.info '-- SS ROUGH PARSE --'
				#clean_input_files
				
				file_set =[
					{file_name:'Clearwater', template_type:'C1'},
					{file_name:'EffectiveDuration', template_type:'E1'},
					{file_name:'Profitstar', template_type:'P1'}
				]						

				file_set.each do |set|
					prs = ParseRuleSet.new(set[:template_type])
					t = Time.now
					fspec = Dir.glob("#{$inbox}/*#{set[:file_name]}.txt").first
					logger.info "looking at: #{fspec}"
				
					fname = File.basename(fspec)
					outfile = File.open("#{$rough}/rough_#{fname}",'w')
					outlog = File.open("#{$rough}/log_#{fname}",'w')
					hfile = file_to_hash(fspec)

					hfile[:file_array].each do |line|

						h = test_line(line, prs.rules)
						if h == nil then
							#outfile << "NOMATCH\n"
							outlog << "-- no match --\n"
							
						else 
							# frequency analysis, bumps the counter on this filter (slow, needs to search the
							# filterset)
							prs.inc_freq(h[:key])

							case h[:line_category] 
							when 'DATA'
								outfile << "D#{h[:line_type].upcase} #{line}"
								hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
								outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"

							when 'HEADER'
								outfile << line
								hfile[:rough_line] << {line_category:h[:line_category], line_type:h[:line_type], content:line}
								outlog << "#{h[:line_category]}\t#{h[:line_type]}\n"
								
							when 'BLANK'
								#outfile << "BLANK\n"
								outlog << "BLANK\n"
							end
						end
					end
				

					logger.info "#{fname} lines parsed: #{Time.now - t} seconds"
					prs.sort_by_freq
					prs.persist
					prs.report_freq
					

					case set[:template_type]
					when 'E1' then
						parse_E1(hfile)

					when 'P1' then 
						parse_P1(hfile)

					when 'C1' then 
						parse_C1(hfile)

					end
				end
			end

			def regex_split(rows, cols, regex)

				## rows are the array of actual rows
				## cols is the count of column breaks

				# this takes a array of lines and brings back an array split

				delimited_records = []
				logger.info "regex split"
				rows.each do |line|
					patt = regex.match(line)
					if patt != nil then
						delimited_records << vbar(patt[1..cols])+"\n"
					end
				end
				#pp delimited_records
				return delimited_records
			end

			def fixed_width_split(rows, cols)

				## rows are the array of actual rows
				## cols is an array of column breaks

				# this takes a array of lines and brings back an array split

				delimited_records = []
				logger.info "fixed width split"
				rows.each do |line|
					fields = ''
					temp_cols = cols.sort
					while temp_cols.count > 1 do 
						fields += catch_nil(line[temp_cols[0]..temp_cols[1]-1].strip) +'|' 
						temp_cols.shift
					end
					fields += catch_nil(line[temp_cols[0]..-1]).lstrip  # last item
					#pp "last item: #{catch_nil(line[temp_cols[0]..-1]).lstrip}"

					delimited_records << fields
				end
				return delimited_records
			end

			def comma_split(rows)

				## rows are the array of actual rows
				## d is the delimiter

				delimited_records = []
				logger.info "comma split"
				rows.each do |line|
					fields = line.split(/,/)
					delimited_records << vbar(fields)
				end
				#pp delimited_records.last
				return delimited_records
			end

			def csv_split(rows)

				## rows are the array of actual rows
				## d is the delimiter

				begin
					delimited_records = []
					logger.info "csv split"
					rows.each do |line|
						#cline = line.gsub(/\"/,'')
						delimited_records << tight_vbar(line.parse_csv)		
					end
				rescue => e
					puts "#{e}"
					puts "broken line: #{line}"
				end
				#pp delimited_records.last
				return delimited_records
			end

			def tab_split(rows)

				## rows are the array of actual rows
				## d is the delimiter

				delimited_records = []
				logger.info "tab split"
				rows.each do |line|
					fields = line.split(/\t/)
					delimited_records << vbar(fields)
				end
				#pp delimited_records.last
				return delimited_records
			end

			def standard_split(rows, d=" ")

				## rows are the array of actual rows
				## d is the delimiter

				# this takes a array of lines and brings back an array split

				delimited_records = []
				logger.info "standard split"
				rows.each do |line|
					fields = line.split(/\s/)
					raw_record = fields.select {|item| item.length > 1}
					delimited_records << vbar(raw_record)+"\n"
				end
				pp delimited_records.last
				return delimited_records
			end

			def parse_A2(h)

				# For A200 there are four types of records and two modes. In the input file data is split
				# into two records. This fine parse will accept both lines and concat them. SBA pool data
				# needs to be dealt with. Should we vbar everything here?

				h[:fine_line] = []
				i = 0

				logger.info "A200 line count #{h[:rough_line].count}"
				
				while i+1 < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					a = catch_nil(h[:rough_line][i][:line_type])
					b = catch_nil(h[:rough_line][i+1][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category]) 
				
					case cat
					when 'HEADER'
						new_content = h[:rough_line][i][:content]
						patt = /Group:.*(\(\w{2,4}\)).*Type:.*(\(\w{2,4}\))/.match(new_content)
						header = "#{pad_to(patt[1],10)}#{pad_to(patt[2],10)}"

					when 'DATA'
						if a == 'type005' and b == 'type006' then
							# we have a match
							current_mode = 'normal'
							new_content = header + h[:rough_line][i][:content].chop + "    " + h[:rough_line][i+1][:content]
							h[:fine_line] <<  {mode:current_mode, content:new_content}
							pool_a = h[:rough_line][i][:content][0..38]
							pool_b = h[:rough_line][i+1][:content][0..38]
							#logger.info "match: #{h[:fine_line].last}"
							#logger.info "sba pool: #{sba_pool}"
						end
						if a == 'type007' and b == 'type008' then
							# we have a situation
							current_mode = 'extended'
							new_content = header +pool_a + h[:rough_line][i][:content].lstrip.chop + "    "+ pool_b + h[:rough_line][i+1][:content].lstrip
							h[:fine_line] <<  {mode:current_mode, content:new_content}
						end
					end
					i += 1
					
				end
				h[:fixed_widths] = [0,10,20,30,59,70,75,84,99,108,121,136,155,165,173,185,194,205,210,219,229,243,256,271]
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])
				pp h[:fine_line].last

				## Create yml
				persist_it(h)
				return h
			end

			def parse_C1(h)

				# There is only one rough regex. Simple

				h[:fine_line] = []
				i = 0

				logger.info "Clearwater line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type =='type000' 

						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
						#logger.info "match: #{h[:fine_parse].last}"
					end
					i += 1
				end
				
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = tab_split(content_array)

				## Create yml
				persist_it(h)
				return h
			end

			def parse_E1(h)

				# There is only one rough regex. Simple

				h[:fine_line] = []
				i = 0

				logger.info "EffectiveDuration line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type =='type000' 
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
	
					end
					i += 1
				end
				
				## now delimiting
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = tab_split(content_array)

				## Create yml
				persist_it(h)
				return h
			end

			def parse_M1(h)

				# There are two types of record here, each captured by the same rough regex. We distinguish
				# them by their preceding headers and split them into the two modes. The first mode has a
				# header that begins with the word 'Maturities' and the second with 'Par Value'. Those are
				# thus the mode names.


				#hfile is all the information and content gotten in the rough parse
				h[:fine_line] = []
				i = 0

				logger.info "M020 line count #{h[:rough_line].count}"
				current_mode = 'unknown'
				while i < h[:rough_line].count do


					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'HEADER' then
						case type 
						when 'type003' 
							current_mode = 'ParValue' 
						when 'type002'
							current_mode = 'Maturities'
						end
					elsif cat == 'DATA' and type =='type001' 
						# we have a match
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content} if current_mode == 'Maturities'
						#logger.info "match: #{h[:fine_parse].last}"
					end
					i += 1
					
				end
				#logger.info h[:fine_parse]
				h[:fixed_widths] = [0,14,29,49,72,94,117]
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])

				## Create yml
				persist_it(h)
				return h
			end

			def parse_M2(h)

				# There is only one rough regex. Simple

				h[:fine_line] = []
				i = 0

				logger.info "M030-1 line count #{h[:rough_line].count}"
				current_mode = 'normal'
				header = pad_to('didnt find crap',32)
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'HEADER' and type == 'type003'
						new_content = h[:rough_line][i][:content]
						patt = /(-->>>)\W+(.*)\W+(<<<--)/.match(new_content)
						header = pad_to(patt[2],32)
						#logger.info "found header #{header}"

					elsif cat == 'DATA' and type =='type004' 
						# we have a match
						new_content = header + h[:rough_line][i][:content]
						#pp new_content
						h[:fine_line] <<  {mode:current_mode, content:new_content}
					end
					i += 1
				end
				
				## now delimiting
				h[:fixed_widths] = [0,13,21,37,66,79,93,109,132,155,179,191]  #not used
				h[:regex] = /(.{30})\W+(\w{3}\/\d{2})\W+(\w)\W+(\w{9})\W+(.{,22})\W+(\d{1,2}\.\d{3})\W+(\d{2}\/\d{2}\/\d{2})\W+(\d{10})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(\d{1,2}\.\d{3})\W+(\d{1,2}\.\d{3})\W?/
				h[:cols] = 13
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = regex_split(content_array, h[:cols], h[:regex])
				pp h[:fine_line].last

				## create yml
				persist_it(h)
				return h
			end

			def parse_M3(h)

				# There is only one rough regex. Simple

				h[:fine_line] = []
				i = 0

				logger.info "M030-2 line count #{h[:rough_line].count}"
				current_mode = 'normal'
				header = pad_to('didnt find crap',45)
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'HEADER' and type == 'type003'
						new_content = h[:rough_line][i][:content]
						patt = /(-->>>)\W+(.*)\W+(<<<--)/.match(new_content)
						header = pad_to(patt[2],45)
						#logger.info "found header #{header}"

					elsif cat == 'DATA' and type == 'type004' 
						# we have a match
						new_content = header + h[:rough_line][i][:content]
						#pp new_content
						h[:fine_line] <<  {mode:current_mode, content:new_content}
						#logger.info "match: #{h[:fine_parse].last}"
					end
					i += 1
					
				end
				
				# h[:fixed_widths] = [0,12,21,37,66,78,93,110,133,154,183,195]
				h[:regex] = /(.{43})\W+(\w{3}\/\d{2})\W+(\w)\W+(\w{9})\W+(.{10,22})\W+(\d{1,2}\.\d{3})\W+(\d{2}\/\d{2}\/\d{2})\W+(\d{10})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(\d{1,2}\.\d{3})\W+(\d{1,2}\.\d{3})\W?/
				h[:cols] = 13
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = regex_split(content_array, h[:cols], h[:regex])

				## create yml
				persist_it(h)
				return h
			end

			def parse_M4(h)

				# There are four different line types here that we want to capture

				# init
				h[:fine_line] = []
				content_array = []
				i = 0

				logger.info "M030 line count #{h[:rough_line].count}"
				current_mode = 'type004'
				while i < h[:rough_line].count do

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'HEADER'
						new_content = h[:rough_line][i][:content]
						patt = /Version:\W+(\w{2,5})/.match(new_content)
						header_type = pad_to(patt[1],8)

					elsif cat == 'DATA'
	
						case type
						when 'type002' # 1month
              new_content = h[:rough_line][i][:content]
              patt = /^\W?\W+(.{,9})\b/.match(new_content)
              qualifiers = pad_to("0 Months   1 Month",24)    
              #h[:fine_line] <<  {mode:type, content:header_type+'0 Months '+new_content}

            when 'type003' 
              # do nothing this is a total line
              #new_content = h[:rough_line][i][:content]
              #h[:fine_line] <<  {mode:type, content:new_content}

            when 'type004' 
              new_content = h[:rough_line][i][:content]
              patt = /^\W?\W+(.{,9})\W+(.{,9})\b/.match(new_content)
              qualifiers = "#{pad_to(patt[1],12)}#{pad_to(patt[2],12)}"
              #h[:fine_line] <<  {mode:type, content:header_type+new_content}

            when 'type007' 
              new_content =  h[:rough_line][i][:content].lstrip
              #h[:fine_line] <<  {mode:type, content:new_content}

            when 'type008' 
              new_content = h[:rough_line][i][:content]
              patt = /^\W+(.+Beyond)/.match(new_content)
              qualifiers = "#{pad_to(catch_nil(patt[1]),12)}#{pad_to(catch_nil(patt[2]),12)}"
              #h[:fine_line] <<  {mode:type, content:header_type+new_content}
            end           

						content_array << header_type + qualifiers + new_content + '\r\n'
					end

					i += 1
					
				end

				#h[:regex] = /^\W?(.{10,20})(|.+Years|.+Month|.+Months|.+Beyond)\W+(\w{2,4})\W+(\w{2,4})\W+(\d{,2})\W+(.{,11}\.\d{2})\W+(.{,11}\.\d{2})\W+(.{,11}\.\d{2})\W+(.{,3}\.\d{,3})\W+(.{,3}\.\d{,3})\W+(.{,3}\.\d{,3})\W+Yrs\W+(.{,3}\.\d{,3})\W+Yrs\W?(\w{,8})\W?$/
				h[:regex] = /\W?(\w{2,5})\W+(.{,9})\W+(.{,10})\W+(\w{3,4})\W+(\w{3,4})\W+(\d{,3})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{,3}\.\d{,3})\W+(.{,3}\.\d{,3})\W+(.{,3}\.\d{,3})\W+Yrs\W+(.{,3}\.\d{,3})\W+Yrs\W?/
				h[:cols] = 13
				h[:fine_line] = regex_split(content_array, h[:cols], h[:regex])
				pp h[:fine_line].last

				## create yml
				persist_it(h)
				return h
			end

			def parse_M5(h)

				# There are two types of record here, each captured by the same rough regex. We distinguish
				# them by their preceding headers and split them into the two modes. The first mode has a
				# header that begins with the word 'Maturities' and the second with 'Par Value'. Those are
				# thus the mode names.


				#hfile is all the information and content gotten in the rough parse
				h[:fine_line] = []
				i = 0

				logger.info "M020 line count #{h[:rough_line].count}"
				current_mode = 'unknown'
				while i < h[:rough_line].count do


					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'HEADER' then
						case type 
						when 'type003' 
							current_mode = 'ParValue' 
						when 'type002'
							current_mode = 'Maturities'
						end
					elsif cat == 'DATA' then
						case type
						when 'type001' 
							# we have a match 
							new_content = h[:rough_line][i][:content]
							h[:fine_line] <<  {mode:current_mode, content:new_content} if current_mode == 'ParValue'
							
					  when 'type002' 
							# we have a match (short record)
							new_content = h[:rough_line][i][:content] + ".\r\n"
							h[:fine_line] <<  {mode:current_mode, content:new_content} if current_mode == 'ParValue'
							
							#logger.info "match: #{h[:fine_parse].last}"
						end
					end
					i += 1
					
				end
				h[:fixed_widths] = [0,14,30,49,69,88,107,124]
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])

				## create yml
				persist_it(h)
				return h
			end

			def parse_P1(h)

				# -- Profitstar --
				# There is only one rough regex. Simple
				#

				h[:fine_line] = []
				i = 0

				logger.info "Profitstar line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type == 'type000' 
						# we have a match
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
				
					end
					i += 1
					
				end
				
				content_array = h[:fine_line].map {|x| x[:content]}
				#pp content_array
				h[:fine_line] = tab_split(content_array)

				## create yml
				persist_it(h)
				return h
			end

			def parse_P2(h)

				# For P010 there are two types of records and one mode. In the input file data is split
				# into two records. This fine parse will accept both lines and concat them. 


				h[:fine_line] = []
				i = 0
				current_mode = 'weird'
				logger.info "P010 line count #{h[:rough_line].count}"
				
				while i+1 < h[:rough_line].count do

					a = catch_nil(h[:rough_line][i][:line_type])
					b = catch_nil(h[:rough_line][i+1][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])
				
					case cat
					when 'HEADER'
						new_content = h[:rough_line][i][:content]
						patt = /------.*(\(\w{2,6}\)).*-----/.match(new_content)
						header = "#{pad_to(patt[1],10)}"

					when 'DATA'
						if a == 'type001' and b == 'type002' then
							# we have a match
							new_content = header + h[:rough_line][i][:content].chop + "     " + h[:rough_line][i+1][:content]
							h[:fine_line] <<  {mode:'normal', content:new_content}
					
						end
					end
					i += 1
					
				end

				h[:fixed_widths] = [0,10,20,39,48,59,71,102,108,128,146,162,175,187,193,209,218,229,238,246,263]
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])
				pp h[:fine_line].last

				## create yml
				persist_it(h)
				return h
			end

			def parse_R1(h)

				# There is only one rough regex. Simple

				h[:fine_line] = []
				i = 0

				logger.info "R010-3 line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type =='type000' 
						# we have a match
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
						#logger.info "match: #{h[:fine_parse].last}"
					end
					i += 1
					
				end
				
				h[:fixed_widths] = [0,5,11,21,53,60,69,78,90,99,118]
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])

				## create yml
				persist_it(h)
				return h
			end

			def parse_S1(h)

				# S1 looks like A2 where there are four types of records and two modes. In the input file
				# data is split into two records. This fine parse will accept both lines and concat them.
				# SBA pool data needs to be dealt with. Should we vbar everything here?

				h[:fine_line] = []
				i = 0
				current_mode = 'weird'
				logger.info "S1 line count #{h[:rough_line].count}"
				
				while i+1 < h[:rough_line].count do

					a = catch_nil(h[:rough_line][i][:line_type])
					b = catch_nil(h[:rough_line][i+1][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])
				
					case cat
					when 'HEADER'
						new_content = h[:rough_line][i][:content]
						patt = /Group:.*\((\w{2,4})\).*Type:.*\((\w{2,4})\)/.match(new_content) # rid parens
						header = "#{pad_to(patt[1],10)}#{pad_to(patt[2],10)}" 

					when 'DATA'
						if a == 'type001' and b == 'type002' then
							# we have a match
							new_content = header + h[:rough_line][i][:content].chop + "     " + h[:rough_line][i+1][:content]
							h[:fine_line] <<  {mode:'normal', content:new_content}
							#logger.info "match: #{h[:fine_line].last}"
							#logger.info "sba pool: #{sba_pool}"
						end
					end
					i += 1
					
				end
				do_regex = true

				if do_regex then

					h[:regex] = /\W?(\w{3,5})\W+(\w{3,5})\W+(\d{10})\W+(.{10,25})\W+(\d{2}\/\d{2}\/\d{2})\W+(SELL)\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(\w{9})\W+(\d{1,2}\.\d{2,5}\W+\d{2}\/\d{2}\/\d{4})\W+(\d{2}\/\d{2}\/\d{2})\W+(\d{2}\/\d{2}\/\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})\W+(.{3,11}\.\d{2})/
					h[:cols] = 18
					content_array = h[:fine_line].map {|x| x[:content]}
					h[:fine_line] = regex_split(content_array, h[:cols], h[:regex])

				else
					#h[:fixed_widths] = [0,10,20,31,62,76,85,103,120,138,157,169,178,189,198,217,243,257,275]
					h[:fixed_widths] = [0,10,20,31,62,76,85,103,120,138,157,169,189,198,217,243,257,275]
					content_array = h[:fine_line].map {|x| x[:content]}
					h[:fine_line] = fixed_width_split(content_array, h[:fixed_widths])
				end
				
				## create yml
				persist_it(h)
				#binding.pry 
				return h
			end

			def parse_V1(h)

				# There is only one rough comma. Simple

				h[:fine_line] = []
				i = 0

				logger.info "VS_A line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type =='type001' 
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
	
					end
					i += 1
				end
				
				## now delimiting
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = csv_split(content_array)

				## Create yml
				persist_it(h)
				return h
			end

			def parse_V2(h)

				# There is only one rough comma. Simple

				h[:fine_line] = []
				i = 0

				logger.info "VS_B line count #{h[:rough_line].count}"
				current_mode = 'normal'
				while i < h[:rough_line].count do

					# roll through the arrays
					#logger.info "index: #{i}"
					#logger.info "rough line: #{h[:rough_line][i]}\n"

					type = catch_nil(h[:rough_line][i][:line_type])
					cat = catch_nil(h[:rough_line][i][:line_category])

					if cat == 'DATA' and type =='type001' 
						new_content = h[:rough_line][i][:content]
						h[:fine_line] <<  {mode:current_mode, content:new_content}
	
					end
					i += 1
				end
				
				## now delimiting
				content_array = h[:fine_line].map {|x| x[:content]}
				h[:fine_line] = csv_split(content_array)

				## Create yml
				persist_it(h)
				return h
			end

			def pad_to(item, new_size)

				if item.length < new_size then
					paddo = new_size - item.length
					while paddo > 0 do
						item += ' '
						paddo -= 1
					end
				end

				return item
			end

			def test_line(line, filter_set)
				answer = nil
				filter_set.each do |filter|
					patt = filter[:regex].match(line)
					if patt != nil then
						#answer = filter[:line_category]
						answer = filter
						#logger.info "#{answer} -- #{line}"
						return answer
					end				
				end		
				return answer
			end
	

		end
	end
end
