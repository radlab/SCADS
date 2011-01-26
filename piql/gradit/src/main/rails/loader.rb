#gi1 = GlobalId.new
#gi1.row_id = 1
#gi1.id_counter = 1
#gi1.save

# Sample user
# username: allen
# password: allenc
u = User.new
u.login = "allen"
u.email = "example@example.com"
u.crypted_password = "44d1f9bd456ae2fb8558b1c4877227fbb16cf4e9"
u.salt = "756b488e613c4899fa16bf3e6c350e39c626296d"
u.save

u = User.new
u.put("login", "amber")
u.save



masterwordlist = [
["indulged", "gratify give way to satisfy allow oneself"],
["vex", "annoy distress trouble"],
["chastened", "corrected, punished"],
["discerned", "see with an effort but clearly"]
]

allwords = []

for wordobj in masterwordlist do
	nw = Word.new
	nw.word = wordobj[0]
	nw.definition = wordobj[1]
	nw.save
	allwords << nw
end


# Set 'content' here as a full book corpus.
# This is really, really slow if you have a really big book.
# This is a lot of preprocessing.

b1 = Book.new
b1.name = "Wuthering Heights"
b1.save

wl1 = Wordlist.new
wl1.put("name", "amber")
wl1.save

wl2 = Wordlist.new
wl2.put("name", "hello")
wl2.save

ww1 = WordsWordlist.new
ww1.put("word_word", allwords[0])
ww1.put("wordlist_name", wl2)
ww1.save($piql_env)

ww2 = WordsWordlist.new
ww2.put("word_word", allwords[1])
ww2.put("wordlist_name", wl2)
ww2.save($piql_env)

ww3 = WordsWordlist.new
ww3.put("word_word", allwords[2])
ww3.put("wordlist_name", wl2)
ww3.save($piql_env)

ww4 = WordsWordlist.new
ww4.put("word_word", allwords[3])
ww4.put("wordlist_name", wl2)
ww4.save($piql_env)
=begin
bookline1 = BookLine.new
bookline1.put("line", "But one day, when she had been peculiarly wayward, rejecting her breakfast, complaining that the servants did not do what she told them; that the mistress would allow her to be nothing in the house, and Edgar neglected her; that she had caught a cold with the doors being left open, and we let the parlour fire go out on purpose to vex her, with a hundred yet more frivolous accusations, Mrs. Linton peremptorily insisted that she should get to bed; and, having scolded her heartily, threatened to send for the doctor.")
#bookline1.put("linenum", 1)
bookline1.put("source", b1)
bookline1.save($piql_env)

c1 = Context.new
c1.context_id = java.lang.Integer.new(1)
c1.wordline = bookline1.line
c1.before = ""
c1.after = ""
c1.book_name = b1
c1.word_word = nw2
c1.save

wr1 = WordReference.new
wr1.word_word = nw2
wr1.context_id = c1
wr1.save

bookline2 = BookLine.new
bookline2.put("line", "That sounds ill-natured: but she was so proud it became really impossible to pity her distresses, till she should be chastened into more humility.")
#bookline2.put("linenum", 2)
bookline2.put("source", b1)
bookline2.save($piql_env)

c2 = Context.new
c2.context_id = java.lang.Integer.new(2)
c2.wordline = bookline2.line
c2.before = ""
c2.after = ""
c2.book_name = b1
c2.word_word = nw3
c2.save

wr2 = WordReference.new
wr2.word_word = nw3
wr2.context_id = c2
wr2.save

bookline3 = BookLine.new
bookline3.put("line", "‘It’s well the hellish villain has kept his word!’ growled my future host, searching the darkness beyond me in expectation of discovering Heathcliff; and then he indulged in a soliloquy of execrations, and threats of what he would have done had the ‘fiend’ deceived him.")
#bookline3.put("linenum", 3)
bookline3.put("source", b1)
bookline3.save($piql_env)

c3 = Context.new
c3.context_id = java.lang.Integer.new(3)
c3.wordline = bookline3.line
c3.before = ""
c3.after = ""
c3.book_name = b1
c3.word_word = nw1
c3.save

wr3 = WordReference.new
wr3.word_word = nw1
wr3.context_id = c3
wr3.save


bookline4 = BookLine.new
bookline4.put("line", "As it spoke, I discerned, obscurely, a child's face looking through the window.  Terror made me cruel; and, finding it useless to attempt shaking the creature off, I pulled its wrist on to the broken pane, and rubbed it to and fro till the blood ran down and soaked the bedclothes: still it wailed, 'Let me in!' and maintained its tenacious gripe, almost maddening me with fear.")
bookline4.put("source", b1)
bookline4.save($piql_env)

c4 = Context.new
c4.context_id = java.lang.Integer.new(4)
c4.wordline = bookline4.line
c4.before = ""
c4.after = ""
c4.book_name = b1
c4.word_word = nw4
c4.save

wr4 = WordReference.new
wr4.word_word = nw1
wr4.context_id = c4
wr4.save
=end

puts "Starting work on preparsing text..."

content = "
I took a seat at the end of the hearthstone opposite that towards which
my landlord advanced, and filled up an interval of silence by attempting
to caress the canine mother, who had left her nursery, and was sneaking
wolfishly to the back of my legs, her lip curled up, and her white teeth
watering for a snatch.  My caress provoked a long, guttural gnarl.

'You'd better let the dog alone,' growled Mr. Heathcliff in unison,
checking fiercer demonstrations with a punch of his foot.  'She's not
accustomed to be spoiled--not kept for a pet.'  Then, striding to a side
door, he shouted again, 'Joseph!'

Joseph mumbled indistinctly in the depths of the cellar, but gave no
intimation of ascending; so his master dived down to him, leaving me _vis-
a-vis_ the ruffianly bitch and a pair of grim shaggy sheep-dogs, who
shared with her a jealous guardianship over all my movements.  Not
anxious to come in contact with their fangs, I sat still; but, imagining
they would scarcely understand tacit insults, I unfortunately indulged in
winking and making faces at the trio, and some turn of my physiognomy so
irritated madam, that she suddenly broke into a fury and leapt on my
knees.  I flung her back, and hastened to interpose the table between us.
This proceeding aroused the whole hive: half-a-dozen four-footed fiends,
of various sizes and ages, issued from hidden dens to the common centre.
I felt my heels and coat-laps peculiar subjects of assault; and parrying
off the larger combatants as effectually as I could with the poker, I was
constrained to demand, aloud, assistance from some of the household in re-
establishing peace.

Mr. Heathcliff and his man climbed the cellar steps with vexatious
phlegm: I don't think they moved one second faster than usual, though the
hearth was an absolute tempest of worrying and yelping.  Happily, an
inhabitant of the kitchen made more despatch: a lusty dame, with tucked-
up gown, bare arms, and fire-flushed cheeks, rushed into the midst of us
flourishing a frying-pan: and used that weapon, and her tongue, to such
purpose, that the storm subsided magically, and she only remained,
heaving like a sea after a high wind, when her master entered on the
scene.

'What the devil is the matter?' he asked, eyeing me in a manner that I
could ill endure, after this inhospitable treatment.

I walked round the yard, and through a wicket, to another door, at which
I took the liberty of knocking, in hopes some more civil servant might
show himself.  After a short suspense, it was opened by a tall, gaunt
man, without neckerchief, and otherwise extremely slovenly; his features
were lost in masses of shaggy hair that hung on his shoulders; and _his_
eyes, too, were like a ghostly Catherine's with all their beauty
annihilated.

'What's your business here?' he demanded, grimly.  'Who are you?'

'My name was Isabella Linton,' I replied.  'You've seen me before, sir.
I'm lately married to Mr. Heathcliff, and he has brought me here--I
suppose, by your permission.'

'Is he come back, then?' asked the hermit, glaring like a hungry wolf.

'Yes--we came just now,' I said; 'but he left me by the kitchen door; and
when I would have gone in, your little boy played sentinel over the
place, and frightened me off by the help of a bull-dog.'

'It's well the hellish villain has kept his word!' growled my future
host, searching the darkness beyond me in expectation of discovering
Heathcliff; and then he indulged in a soliloquy of execrations, and
threats of what he would have done had the 'fiend' deceived him.

I repented having tried this second entrance, and was almost inclined to
slip away before he finished cursing, but ere I could execute that
intention, he ordered me in, and shut and re-fastened the door.  There
was a great fire, and that was all the light in the huge apartment, whose
floor had grown a uniform grey; and the once brilliant pewter-dishes,
which used to attract my gaze when I was a girl, partook of a similar
obscurity, created by tarnish and dust.  I inquired whether I might call
the maid, and be conducted to a bedroom!  Mr. Earnshaw vouchsafed no
answer.  He walked up and down, with his hands in his pockets, apparently
quite forgetting my presence; and his abstraction was evidently so deep,
and his whole aspect so misanthropical, that I shrank from disturbing him
again.

The twelve years, continued Mrs. Dean, following that dismal period were
the happiest of my life: my greatest troubles in their passage rose from
our little lady's trifling illnesses, which she had to experience in
common with all children, rich and poor.  For the rest, after the first
six months, she grew like a larch, and could walk and talk too, in her
own way, before the heath blossomed a second time over Mrs. Linton's
dust.  She was the most winning thing that ever brought sunshine into a
desolate house: a real beauty in face, with the Earnshaws' handsome dark
eyes, but the Lintons' fair skin and small features, and yellow curling
hair.  Her spirit was high, though not rough, and qualified by a heart
sensitive and lively to excess in its affections.  That capacity for
intense attachments reminded me of her mother: still she did not resemble
her: for she could be soft and mild as a dove, and she had a gentle voice
and pensive expression: her anger was never furious; her love never
fierce: it was deep and tender.  However, it must be acknowledged, she
had faults to foil her gifts.  A propensity to be saucy was one; and a
perverse will, that indulged children invariably acquire, whether they be
good tempered or cross.  If a servant chanced to vex her, it was
always--'I shall tell papa!'  And if he reproved her, even by a look, you
would have thought it a heart-breaking business: I don't believe he ever
did speak a harsh word to her.  He took her education entirely on
himself, and made it an amusement.  Fortunately, curiosity and a quick
intellect made her an apt scholar: she learned rapidly and eagerly, and
did honour to his teaching.

Till she reached the age of thirteen she had not once been beyond the
range of the park by herself.  Mr. Linton would take her with him a mile
or so outside, on rare occasions; but he trusted her to no one else.
Gimmerton was an unsubstantial name in her ears; the chapel, the only
building she had approached or entered, except her own home.  Wuthering
Heights and Mr. Heathcliff did not exist for her: she was a perfect
recluse; and, apparently, perfectly contented.  Sometimes, indeed, while
surveying the country from her nursery window, she would observe--

'Ellen, how long will it be before I can walk to the top of those hills?
I wonder what lies on the other side--is it the sea?'

'No, Miss Cathy,' I would answer; 'it is hills again, just like these.'

'And what are those golden rocks like when you stand under them?' she
once asked.

A letter, edged with black, announced the day of my master's return,
Isabella was dead; and he wrote to bid me get mourning for his daughter,
and arrange a room, and other accommodations, for his youthful nephew.
Catherine ran wild with joy at the idea of welcoming her father back; and
indulged most sanguine anticipations of the innumerable excellencies of
her 'real' cousin.  The evening of their expected arrival came.  Since
early morning she had been busy ordering her own small affairs; and now
attired in her new black frock--poor thing! her aunt's death impressed
her with no definite sorrow--she obliged me, by constant worrying, to
walk with her down through the grounds to meet them.

'Linton is just six months younger than I am,' she chattered, as we
strolled leisurely over the swells and hollows of mossy turf, under
shadow of the trees.  'How delightful it will be to have him for a
playfellow!  Aunt Isabella sent papa a beautiful lock of his hair; it was
lighter than mine--more flaxen, and quite as fine.  I have it carefully
preserved in a little glass box; and I've often thought what a pleasure
it would be to see its owner.  Oh! I am happy--and papa, dear, dear papa!
Come, Ellen, let us run! come, run.'

She ran, and returned and ran again, many times before my sober footsteps
reached the gate, and then she seated herself on the grassy bank beside
the path, and tried to wait patiently; but that was impossible: she
couldn't be still a minute.

'How long they are!' she exclaimed.  'Ah, I see, some dust on the
road--they are coming!  No!  When will they be here?  May we not go a
little way--half a mile, Ellen, only just half a mile?  Do say Yes: to
that clump of birches at the turn!'

'You can't alter what you've done,' he replied pettishly, shrinking from
her, 'unless you alter it for the worse by teasing me into a fever.'

'Well, then, I must go?' she repeated.

'Let me alone, at least,' said he; 'I can't bear your talking.'

She lingered, and resisted my persuasions to departure a tiresome while;
but as he neither looked up nor spoke, she finally made a movement to the
door, and I followed.  We were recalled by a scream.  Linton had slid
from his seat on to the hearthstone, and lay writhing in the mere
perverseness of an indulged plague of a child, determined to be as
grievous and harassing as it can.  I thoroughly gauged his disposition
from his behaviour, and saw at once it would be folly to attempt
humouring him.  Not so my companion: she ran back in terror, knelt down,
and cried, and soothed, and entreated, till he grew quiet from lack of
breath: by no means from compunction at distressing her.

'I shall lift him on to the settle,' I said, 'and he may roll about as he
pleases: we can't stop to watch him.  I hope you are satisfied, Miss
Cathy, that you are not the person to benefit him; and that his condition
of health is not occasioned by attachment to you.  Now, then, there he
is!  Come away: as soon as he knows there is nobody by to care for his
nonsense, he'll be glad to lie still.'

She placed a cushion under his head, and offered him some water; he
rejected the latter, and tossed uneasily on the former, as if it were a
stone or a block of wood.  She tried to put it more comfortably.

'I can't do with that,' he said; 'it's not high enough.'

Catherine brought another to lay above it.

'That's too high,' murmured the provoking thing.

'How must I arrange it, then?' she asked despairingly.

He twined himself up to her, as she half knelt by the settle, and
converted her shoulder into a support.

We had all remarked, during some time, that Miss Linton fretted and pined
over something.  She grew cross and wearisome; snapping at and teasing
Catherine continually, at the imminent risk of exhausting her limited
patience.  We excused her, to a certain extent, on the plea of
ill-health: she was dwindling and fading before our eyes.  But one day,
when she had been peculiarly wayward, rejecting her breakfast,
complaining that the servants did not do what she told them; that the
mistress would allow her to be nothing in the house, and Edgar neglected
her; that she had caught a cold with the doors being left open, and we
let the parlour fire go out on purpose to vex her, with a hundred yet
more frivolous accusations, Mrs. Linton peremptorily insisted that she
should get to bed; and, having scolded her heartily, threatened to send
for the doctor.  Mention of Kenneth caused her to exclaim, instantly,
that her health was perfect, and it was only Catherine's harshness which
made her unhappy.

'How can you say I am harsh, you naughty fondling?' cried the mistress,
amazed at the unreasonable assertion.  'You are surely losing your
reason.  When have I been hash, tell me?'

'Yesterday,' sobbed Isabella, 'and now!'

'Yesterday!' said her sister-in-law.  'On what occasion?'

'In our walk along the moor: you told me to ramble where I pleased, while
you sauntered on with Mr. Heathcliff?'

'To hear you, people might think you were the mistress!' she cried.  'You
want setting down in your right place!  Heathcliff, what are you about,
raising this stir?  I said you must let Isabella alone!--I beg you will,
unless you are tired of being received here, and wish Linton to draw the
bolts against you!'

'God forbid that he should try!' answered the black villain.  I detested
him just then.  'God keep him meek and patient!  Every day I grow madder
after sending him to heaven!'

'Hush!' said Catherine, shutting the inner door!  'Don't vex me.  Why
have you disregarded my request?  Did she come across you on purpose?'

'What is it to you?' he growled.  'I have a right to kiss her, if she
chooses; and you have no right to object.  I am not _your_ husband: _you_
needn't be jealous of me!'

'I'm not jealous of you,' replied the mistress; 'I'm jealous for you.
Clear your face: you sha'n't scowl at me!  If you like Isabella, you
shall marry her.  But do you like her?  Tell the truth, Heathcliff!
There, you won't answer.  I'm certain you don't.'

'And would Mr. Linton approve of his sister marrying that man?' I
inquired.

'Mr. Linton should approve,' returned my lady, decisively.

'Hush!' cried Mrs. Linton.  'Hush, this moment!  You mention that name
and I end the matter instantly by a spring from the window!  What you
touch at present you may have; but my soul will be on that hill-top
before you lay hands on me again.  I don't want you, Edgar: I'm past
wanting you.  Return to your books.  I'm glad you possess a consolation,
for all you had in me is gone.'

'Her mind wanders, sir,' I interposed.  'She has been talking nonsense
the whole evening; but let her have quiet, and proper attendance, and
she'll rally.  Hereafter, we must be cautious how we vex her.'

'I desire no further advice from you,' answered Mr. Linton.  'You knew
your mistress's nature, and you encouraged me to harass her.  And not to
give me one hint of how she has been these three days!  It was heartless!
Months of sickness could not cause such a change!'

I began to defend myself, thinking it too bad to be blamed for another's
wicked waywardness.  'I knew Mrs. Linton's nature to be headstrong and
domineering,' cried I: 'but I didn't know that you wished to foster her
fierce temper!  I didn't know that, to humour her, I should wink at Mr.
Heathcliff.  I performed the duty of a faithful servant in telling you,
and I have got a faithful servant's wages!  Well, it will teach me to be
careful next time.  Next time you may gather intelligence for yourself!'

The twelve years, continued Mrs. Dean, following that dismal period were
the happiest of my life: my greatest troubles in their passage rose from
our little lady's trifling illnesses, which she had to experience in
common with all children, rich and poor.  For the rest, after the first
six months, she grew like a larch, and could walk and talk too, in her
own way, before the heath blossomed a second time over Mrs. Linton's
dust.  She was the most winning thing that ever brought sunshine into a
desolate house: a real beauty in face, with the Earnshaws' handsome dark
eyes, but the Lintons' fair skin and small features, and yellow curling
hair.  Her spirit was high, though not rough, and qualified by a heart
sensitive and lively to excess in its affections.  That capacity for
intense attachments reminded me of her mother: still she did not resemble
her: for she could be soft and mild as a dove, and she had a gentle voice
and pensive expression: her anger was never furious; her love never
fierce: it was deep and tender.  However, it must be acknowledged, she
had faults to foil her gifts.  A propensity to be saucy was one; and a
perverse will, that indulged children invariably acquire, whether they be
good tempered or cross.  If a servant chanced to vex her, it was
always--'I shall tell papa!'  And if he reproved her, even by a look, you
would have thought it a heart-breaking business: I don't believe he ever
did speak a harsh word to her.  He took her education entirely on
himself, and made it an amusement.  Fortunately, curiosity and a quick
intellect made her an apt scholar: she learned rapidly and eagerly, and
did honour to his teaching.

Till she reached the age of thirteen she had not once been beyond the
range of the park by herself.  Mr. Linton would take her with him a mile
or so outside, on rare occasions; but he trusted her to no one else.
Gimmerton was an unsubstantial name in her ears; the chapel, the only
building she had approached or entered, except her own home.  Wuthering
Heights and Mr. Heathcliff did not exist for her: she was a perfect
recluse; and, apparently, perfectly contented.  Sometimes, indeed, while
surveying the country from her nursery window, she would observe--

'Ellen, how long will it be before I can walk to the top of those hills?
I wonder what lies on the other side--is it the sea?'

'No, Miss Cathy,' I would answer; 'it is hills again, just like these.'

'And what are those golden rocks like when you stand under them?' she
once asked.

'Aunt Isabella had not you and me to nurse her,' I replied.  'She wasn't
as happy as Master: she hadn't as much to live for.  All you need do, is
to wait well on your father, and cheer him by letting him see you
cheerful; and avoid giving him anxiety on any subject: mind that, Cathy!
I'll not disguise but you might kill him if you were wild and reckless,
and cherished a foolish, fanciful affection for the son of a person who
would be glad to have him in his grave; and allowed him to discover that
you fretted over the separation he has judged it expedient to make.'

'I fret about nothing on earth except papa's illness,' answered my
companion.  'I care for nothing in comparison with papa.  And I'll
never--never--oh, never, while I have my senses, do an act or say a word
to vex him.  I love him better than myself, Ellen; and I know it by this:
I pray every night that I may live after him; because I would rather be
miserable than that he should be: that proves I love him better than
myself.'

'Good words,' I replied.  'But deeds must prove it also; and after he is
well, remember you don't forget resolutions formed in the hour of fear.'

As we talked, we neared a door that opened on the road; and my young
lady, lightening into sunshine again, climbed up and seated herself on
the top of the wall, reaching over to gather some hips that bloomed
scarlet on the summit branches of the wild-rose trees shadowing the
highway side: the lower fruit had disappeared, but only birds could touch
the upper, except from Cathy's present station.  In stretching to pull
them, her hat fell off; and as the door was locked, she proposed
scrambling down to recover it.  I bid her be cautious lest she got a
fall, and she nimbly disappeared.  But the return was no such easy
matter: the stones were smooth and neatly cemented, and the rose-bushes
and black-berry stragglers could yield no assistance in re-ascending.  I,
like a fool, didn't recollect that, till I heard her laughing and
exclaiming--'Ellen! you'll have to fetch the key, or else I must run
round to the porter's lodge.  I can't scale the ramparts on this side!'

Mr. Edgar seldom mustered courage to visit Wuthering Heights openly.  He
had a terror of Earnshaw's reputation, and shrunk from encountering him;
and yet he was always received with our best attempts at civility: the
master himself avoided offending him, knowing why he came; and if he
could not be gracious, kept out of the way.  I rather think his
appearance there was distasteful to Catherine; she was not artful, never
played the coquette, and had evidently an objection to her two friends
meeting at all; for when Heathcliff expressed contempt of Linton in his
presence, she could not half coincide, as she did in his absence; and
when Linton evinced disgust and antipathy to Heathcliff, she dared not
treat his sentiments with indifference, as if depreciation of her
playmate were of scarcely any consequence to her.  I've had many a laugh
at her perplexities and untold troubles, which she vainly strove to hide
from my mockery.  That sounds ill-natured: but she was so proud it became
really impossible to pity her distresses, till she should be chastened
into more humility.  She did bring herself, finally, to confess, and to
confide in me: there was not a soul else that she might fashion into an
adviser.

Mr. Hindley had gone from home one afternoon, and Heathcliff presumed to
give himself a holiday on the strength of it.  He had reached the age of
sixteen then, I think, and without having bad features, or being
deficient in intellect, he contrived to convey an impression of inward
and outward repulsiveness that his present aspect retains no traces of.
In the first place, he had by that time lost the benefit of his early
education: continual hard work, begun soon and concluded late, had
extinguished any curiosity he once possessed in pursuit of knowledge, and
any love for books or learning.  His childhood's sense of superiority,
instilled into him by the favours of old Mr. Earnshaw, was faded away.  He
struggled long to keep up an equality with Catherine in her studies, and
yielded with poignant though silent regret: but he yielded completely;
and there was no prevailing on him to take a step in the way of moving
upward, when he found he must, necessarily, sink beneath his former
level.  Then personal appearance sympathised with mental deterioration:
he acquired a slouching gait and ignoble look; his naturally reserved
disposition was exaggerated into an almost idiotic excess of unsociable
moroseness; and he took a grim pleasure, apparently, in exciting the
aversion rather than the esteem of his few acquaintance.

This time, I remembered I was lying in the oak closet, and I heard
distinctly the gusty wind, and the driving of the snow; I heard, also,
the fir bough repeat its teasing sound, and ascribed it to the right
cause: but it annoyed me so much, that I resolved to silence it, if
possible; and, I thought, I rose and endeavoured to unhasp the casement.
The hook was soldered into the staple: a circumstance observed by me when
awake, but forgotten.  'I must stop it, nevertheless!' I muttered,
knocking my knuckles through the glass, and stretching an arm out to
seize the importunate branch; instead of which, my fingers closed on the
fingers of a little, ice-cold hand!  The intense horror of nightmare came
over me: I tried to draw back my arm, but the hand clung to it, and a
most melancholy voice sobbed, 'Let me in--let me in!'  'Who are you?' I
asked, struggling, meanwhile, to disengage myself.  'Catherine Linton,'
it replied, shiveringly (why did I think of _Linton_?  I had read
_Earnshaw_ twenty times for Linton)--'I'm come home: I'd lost my way on
the moor!'  As it spoke, I discerned, obscurely, a child's face looking
through the window.  Terror made me cruel; and, finding it useless to
attempt shaking the creature off, I pulled its wrist on to the broken
pane, and rubbed it to and fro till the blood ran down and soaked the
bedclothes: still it wailed, 'Let me in!' and maintained its tenacious
gripe, almost maddening me with fear.  'How can I!' I said at length.
'Let _me_ go, if you want me to let you in!'  The fingers relaxed, I
snatched mine through the hole, hurriedly piled the books up in a pyramid
against it, and stopped my ears to exclude the lamentable prayer.  I
seemed to keep them closed above a quarter of an hour; yet, the instant I
listened again, there was the doleful cry moaning on!  'Begone!' I
shouted.  'I'll never let you in, not if you beg for twenty years.'  'It
is twenty years,' mourned the voice: 'twenty years.  I've been a waif for
twenty years!'  Thereat began a feeble scratching outside, and the pile
of books moved as if thrust forward.  I tried to jump up; but could not
stir a limb; and so yelled aloud, in a frenzy of fright.  To my
confusion, I discovered the yell was not ideal: hasty footsteps
approached my chamber door; somebody pushed it open, with a vigorous
hand, and a light glimmered through the squares at the top of the bed.  I
sat shuddering yet, and wiping the perspiration from my forehead: the
intruder appeared to hesitate, and muttered to himself.  At last, he
said, in a half-whisper, plainly not expecting an answer, 'Is any one
here?'  I considered it best to confess my presence; for I knew
Heathcliff's accents, and feared he might search further, if I kept
quiet.  With this intention, I turned and opened the panels.  I shall not
soon forget the effect my action produced.

Mrs. Dean raised the candle, and I discerned a soft-featured face,
exceedingly resembling the young lady at the Heights, but more pensive
and amiable in expression.  It formed a sweet picture.  The long light
hair curled slightly on the temples; the eyes were large and serious; the
figure almost too graceful.  I did not marvel how Catherine Earnshaw
could forget her first friend for such an individual.  I marvelled much
how he, with a mind to correspond with his person, could fancy my idea of
Catherine Earnshaw.

'A very agreeable portrait,' I observed to the house-keeper.  'Is it
like?'

'Yes,' she answered; 'but he looked better when he was animated; that is
his everyday countenance: he wanted spirit in general.'

Catherine had kept up her acquaintance with the Lintons since her five-
weeks' residence among them; and as she had no temptation to show her
rough side in their company, and had the sense to be ashamed of being
rude where she experienced such invariable courtesy, she imposed
unwittingly on the old lady and gentleman by her ingenious cordiality;
gained the admiration of Isabella, and the heart and soul of her brother:
acquisitions that flattered her from the first--for she was full of
ambition--and led her to adopt a double character without exactly
intending to deceive any one.  In the place where she heard Heathcliff
termed a 'vulgar young ruffian,' and 'worse than a brute,' she took care
not to act like him; but at home she had small inclination to practise
politeness that would only be laughed at, and restrain an unruly nature
when it would bring her neither credit nor praise.

He had a fixed idea, I guessed by several observations he let fall, that,
as his nephew resembled him in person, he would resemble him in mind; for
Linton's letters bore few or no indications of his defective character.
And I, through pardonable weakness, refrained from correcting the error;
asking myself what good there would be in disturbing his last moments
with information that he had neither power nor opportunity to turn to
account.

We deferred our excursion till the afternoon; a golden afternoon of
August: every breath from the hills so full of life, that it seemed
whoever respired it, though dying, might revive.  Catherine's face was
just like the landscape--shadows and sunshine flitting over it in rapid
succession; but the shadows rested longer, and the sunshine was more
transient; and her poor little heart reproached itself for even that
passing forgetfulness of its cares.

We discerned Linton watching at the same spot he had selected before.  My
young mistress alighted, and told me that, as she was resolved to stay a
very little while, I had better hold the pony and remain on horseback;
but I dissented: I wouldn't risk losing sight of the charge committed to
me a minute; so we climbed the slope of heath together.  Master
Heathcliff received us with greater animation on this occasion: not the
animation of high spirits though, nor yet of joy; it looked more like
fear.

'It is late!' he said, speaking short and with difficulty.  'Is not your
father very ill?  I thought you wouldn't come.'

'_Why_ won't you be candid?' cried Catherine, swallowing her greeting.
'Why cannot you say at once you don't want me?  It is strange, Linton,
that for the second time you have brought me here on purpose, apparently
to distress us both, and for no reason besides!'

Linton shivered, and glanced at her, half supplicating, half ashamed; but
his cousin's patience was not sufficient to endure this enigmatical
behaviour.

'Of dissolving with her, and being more happy still!' he answered.  'Do
you suppose I dread any change of that sort?  I expected such a
transformation on raising the lid--but I'm better pleased that it should
not commence till I share it.  Besides, unless I had received a distinct
impression of her passionless features, that strange feeling would hardly
have been removed.  It began oddly.  You know I was wild after she died;
and eternally, from dawn to dawn, praying her to return to me her spirit!
I have a strong faith in ghosts: I have a conviction that they can, and
do, exist among us!  The day she was buried, there came a fall of snow.
In the evening I went to the churchyard.  It blew bleak as winter--all
round was solitary.  I didn't fear that her fool of a husband would
wander up the glen so late; and no one else had business to bring them
there.  Being alone, and conscious two yards of loose earth was the sole
barrier between us, I said to myself--\"I'll have her in my arms again!  If
she be cold, I'll think it is this north wind that chills _me_; and if
she be motionless, it is sleep.\"  I got a spade from the tool-house, and
began to delve with all my might--it scraped the coffin; I fell to work
with my hands; the wood commenced cracking about the screws; I was on the
point of attaining my object, when it seemed that I heard a sigh from
some one above, close at the edge of the grave, and bending down.  \"If I
can only get this off,\" I muttered, \"I wish they may shovel in the earth
over us both!\" and I wrenched at it more desperately still.  There was
another sigh, close at my ear.  I appeared to feel the warm breath of it
displacing the sleet-laden wind.  I knew no living thing in flesh and
blood was by; but, as certainly as you perceive the approach to some
substantial body in the dark, though it cannot be discerned, so certainly
I felt that Cathy was there: not under me, but on the earth.  A sudden
sense of relief flowed from my heart through every limb.  I relinquished
my labour of agony, and turned consoled at once: unspeakably consoled.
Her presence was with me: it remained while I re-filled the grave, and
led me home.  You may laugh, if you will; but I was sure I should see her
there.  I was sure she was with me, and I could not help talking to her.
Having reached the Heights, I rushed eagerly to the door.  It was
fastened; and, I remember, that accursed Earnshaw and my wife opposed my
entrance.  I remember stopping to kick the breath out of him, and then
hurrying up-stairs, to my room and hers.  I looked round impatiently--I
felt her by me--I could _almost_ see her, and yet I _could not_!  I ought
to have sweat blood then, from the anguish of my yearning--from the
fervour of my supplications to have but one glimpse!  I had not one.  She
showed herself, as she often was in life, a devil to me!  And, since
then, sometimes more and sometimes less, I've been the sport of that
intolerable torture!  Infernal! keeping my nerves at such a stretch that,
if they had not resembled catgut, they would long ago have relaxed to the
feebleness of Linton's.  When I sat in the house with Hareton, it seemed
that on going out I should meet her; when I walked on the moors I should
meet her coming in.  When I went from home I hastened to return; she
_must_ be somewhere at the Heights, I was certain!  And when I slept in
her chamber--I was beaten out of that.  I couldn't lie there; for the
moment I closed my eyes, she was either outside the window, or sliding
back the panels, or entering the room, or even resting her darling head
on the same pillow as she did when a child; and I must open my lids to
see.  And so I opened and closed them a hundred times a night--to be
always disappointed!  It racked me!  I've often groaned aloud, till that
old rascal Joseph no doubt believed that my conscience was playing the
fiend inside of me.  Now, since I've seen her, I'm pacified--a little.  It
was a strange way of killing: not by inches, but by fractions of
hairbreadths, to beguile me with the spectre of a hope through eighteen
years!'
"
contentarray = content.scan(/[^\.\?\!]+[\.\?\!]+[\n]*/)
contentsize = contentarray.size
num = 1
for line in content
	a = BookLine.new
	a.put("line", line)
	a.put("source", b1)
	a.save($piql_env)
	
	for word in allwords
		reg = /\b#{word.word}\b/i
		if (reg.match(line))
			puts line
			c = Context.new
			c.context_id = java.lang.Integer.new(num)
			c.wordline = a.line
			c.before = ""
			c.after = ""
			c.book_name = b1
			c.word_word = word
			c.save
			wr = WordReference.new
			wr.word_word = word
			wr.context_id = c
			wr.save
		end
	end
	num=num+1
end

puts "Done!"