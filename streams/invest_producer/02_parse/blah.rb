TimesToDo=1000
testString = ""
for i in 1..1000
  testString = "abababdedfg"
end

regex1 = /a^(a|b|c|d|e|f|g)+$/
regex2 = /^[a-g]+$/

startTime = Time.new.to_f
for i in 1..TimesToDo
  regex1.match(testString)
end

puts "Alternation takes #{(Time.new.to_f - startTime)} seconds" 


startTime = Time.new.to_f
for i in 1..TimesToDo
  regex2.match(testString)
end

puts  "Character Class takes #{(Time.new.to_f - startTime)} seconds" 
