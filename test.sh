#!/bin/bash
TEMPFILE=~/Code/perfdebug-separate-benchmarks/tempdata.txt
VALUE='	       class="selected"	       ><a href="../../../../articles/a/b/t/Talk%7EAbtsbessingen_690c.html">Discussion</a></li><li id="ca-current"'
fgrep "$VALUE" $TEMPFILE
# fgrep  '<h2><span class="editsection">[<a href="../../../../articles/a/b/u/Talk%7EAbu_Zubaydah_824a.html" title="Edit section: Conversation with CSloat">edit</a>]</span> <span class="mw-headline">Conversation with CSloat</span></h2>' $TEMPFILE
# grep '<h2><span class="editsection">[<a href="../../../../articles/a/b/u/Talk%7EAbu_Zubaydah_824a.html" title="Edit section: Conversation with CSloat">edit</a>]</span> <span class="mw-headline">Conversation with CSloat</span></h2>' $TEMPFILE