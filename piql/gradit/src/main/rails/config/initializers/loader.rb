$AVRO_LOADED = 0
puts "************"
puts "Loader beginning\n\n\n"

require File.join(RAILS_ROOT, "lib/avro_record.rb")

words = [
    ["abscond", "to leave secretly and hide, often to avoid the law"], # 1
    ["caustic", "highly critical"], #2
    ["chicanery", "deception by trickery"], #3
    ["dearth", "lack, scarcity"], # 4
    ["diffident", "lacking self confidence, modest"], #5
    ["efficacy", "effectiveness, capability to produce a desired effect"], #6
    ["guile", "skillful deceit"], #7
    ["laudable", "praiseworthy"], #8
    ["lucid", "clear, translucent"], #9
    ["sanguine", "cheerful, confident"], #10
    ["taciturn", "habitually untalkative or silent"], #11
    ["venerate","great respect or reverence"], #12
    ["vex", "to annoy; to bother; to perplex or puzzle; to debate at length"], #13
    ["volatility", "characteristic of being explosive; fickle"], #14
    ["zeal", "enthusiastic devotion to a cause, ideal or goal"] #15
]

puts "\n\nAdding " + words.size.to_s + " words...\n"
wordid = 1
words.each { |w|
    print "Adding word " + wordid.to_s + ": " + w[0] + ": " + w[1] + "...   "
    #if (Word.createNew(wordid, w[0],w[1],"wordlist"))
    #    puts "Success."
    #else
    #    puts ">>>>> FAILURE : Failed adding word. <<<<<"
    #end
    if Word.createNew(wordid, w[0],w[1],"wordlist")
        puts "Success."
    else
        puts ">>>>> FAILURE : Failed adding word. <<<<<"
    end
    wordid = wordid + 1
}

# wordid as integer, book name as string, linenum as integer, wordline as string

contexts = [

    [13, "Wuthering Heights", 402, "We excused her, to a certain extent, on the plea of ill-health: she was dwindling and fading before our eyes.  But one day, when she had been peculiarly wayward, rejecting her breakfast, complaining that the servants did not do what she told them; that the mistress would allow her to be nothing in the house, and Edgar neglected her; that she had caught a cold with the doors being left open, and we let the parlour fire go out on purpose to vex her, with a hundred yet more frivolous accusations, Mrs. Linton peremptorily insisted that she should get to bed; and, having scolded her heartily, threatened to send for the doctor.  Mention of Kenneth caused her to exclaim, instantly, that her health was perfect, and it was only Catherine’s harshness which made her unhappy."],
    [13, "Wuthering Heights", 812, "‘God forbid that he should try!’ answered the black villain.  I detested him just then.  ‘God keep him meek and patient!  Every day I grow madder after sending him to heaven!’

‘Hush!’ said Catherine, shutting the inner door!  ‘Don’t vex me.  Why have you disregarded my request?  Did she come across you on purpose?’

‘What is it to you?’ he growled.  ‘I have a right to kiss her, if she chooses; and you have no right to object.  I am not your husband: you needn’t be jealous of me!’"],
    [13, "Wuthering Heights", 1069, "‘Her mind wanders, sir,’ I interposed.  ‘She has been talking nonsense the whole evening; but let her have quiet, and proper attendance, and she’ll rally.  Hereafter, we must be cautious how we vex her.’

‘I desire no further advice from you,’ answered Mr. Linton.  ‘You knew your mistress’s nature, and you encouraged me to harass her.  And not to give me one hint of how she has been these three days!  It was heartless!  Months of sickness could not cause such a change!’"],
    [1, "Heart of Darkness", 4420, "I looked at him, lost in astonishment. There he was before me, in motley, as though he had absconded from a troupe of mimes, enthusiastic, fabulous. His very existence was improbable, inexplicable, and altogether bewildering. He was an insoluble problem."],
    [5, "Pride and Prejudice", 3102, "He was anxious to avoid the notice of his cousins, from a conviction that if they saw him depart, they could not fail to conjecture his design, and he was not willing to have the attempt known till its success might be known likewise; for though feeling almost secure, and with reason, for Charlotte had been tolerably encouraging, he was comparatively diffident since the adventure of Wednesday. His reception, however, was of the most flattering kind. Miss Lucas perceived him from an upper window as he walked towards the house, and instantly set out to meet him accidentally in the lane. But little had she dared to hope that so much love and eloquence awaited her there."],
    [6, "Pride and Prejudice", 2201, "'And so ended his affection,' said Elizabeth impatiently. 'There has been many a one, I fancy, overcome in the same way. I wonder who first discovered the efficacy of poetry in driving away love!'

'I have been used to consider poetry as the food of love,' said Darcy.

'Of a fine, stout, healthy love it may. Everything nourishes what is strong already. But if it be only a slight, thin sort of inclination, I am convinced that one good sonnet will starve it entirely away.' "],
    [14, "Pride and Prejudice", 8445, "'Indeed you are mistaken. I have no such injuries to resent. It is not of particular, but of general evils, which I am now complaining. Our importance, our respectability in the world must be affected by the wild volatility, the assurance and disdain of all restraint which mark Lydia's character. Excuse me, for I must speak plainly. If you, my dear father, will not take the trouble of checking her exuberant spirits, and of teaching her that her present pursuits are not to be the business of her life, she will soon be beyond the reach of amendment.'"],
    [8, "Pride and Prejudice", 3841, "'The indirect boast; for you are really proud of your defects in writing, because you consider them as proceeding from a rapidity of thought and carelessness of execution, which, if not estimable, you think at least highly interesting. The power of doing anything with quickness is always prized much by the possessor, and often without any attention to the imperfection of the performance. When you told Mrs. Bennet this morning that if you ever resolved upon quitting Netherfield you should be gone in five minutes, you meant it to be a sort of panegyric, of compliment to yourself—and yet what is there so very laudable in a precipitance which must leave very necessary business undone, and can be of no real advantage to yourself or anyone else?' "],
    [10, "Pride and Prejudice", 11947, "When they were all in the drawing-room, the questions which Elizabeth had already asked were of course repeated by the others, and they soon found that Jane had no intelligence to give. The sanguine hope of good, however, which the benevolence of her heart suggested had not yet deserted her; she still expected that it would all end well, and that every morning would bring some letter, either from Lydia or her father, to explain their proceedings, and, perhaps, announce their marriage. "],
    [11, "Pride and Prejudice", 3584, "'Are you consulting your own feelings in the present case, or do you imagine that you are gratifying mine?'

'Both,' replied Elizabeth archly; 'for I have always seen a great similarity in the turn of our minds. We are each of an unsocial, taciturn disposition, unwilling to speak, unless we expect to say something that will amaze the whole room, and be handed down to posterity with all the eclat of a proverb.'

'This is no very striking resemblance of your own character, I am sure,' said he. 'How near it may be to mine, I cannot pretend to say. You think it a faithful portrait undoubtedly.' "],
    [15, "Wuthering Heights", 885, "In the confluence of the multitude, several clubs crossed; blows, aimed at me, fell on other sconces.  Presently the whole chapel resounded with rappings and counter rappings: every man’s hand was against his neighbour; and Branderham, unwilling to remain idle, poured forth his zeal in a shower of loud taps on the boards of the pulpit, which responded so smartly that, at last, to my unspeakable relief, they woke me.  And what was it that had suggested the tremendous tumult?  What had played Jabez’s part in the row?  Merely the branch of a fir-tree that touched my lattice as the blast wailed by, and rattled its dry cones against the panes!  I listened doubtingly an instant; detected the disturber, then turned and dozed, and dreamt again: if possible, still more disagreeably than before."]
   
]

puts "\n\nAdding " + contexts.size.to_s + " contexts...\n"
contexts.each { |c|
    print "Adding context for word " + c[0].to_s + ", book: " + c[1] + " linenum: " + c[2].to_s + "...   "
    if (WordContext.createNew(c[0],c[1],c[2],c[3]))
        puts "Success."
    else
        puts ">>>>> FAILURE : Failed adding wordcontext. <<<<<"
    end
}

# TODO: MORE CONTEXTS

puts "\n\nAdding wordlist(s)...\n"
if WordList.createNew("wordlist")
  puts "Success."
else
    puts ">>>>> FAILURE : Failed adding wordlist. <<<<<"
end

puts "\n\n\nLoader finished"
puts "************"