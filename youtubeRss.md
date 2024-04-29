

The default feed address for playlists: https://www.youtube.com/feeds/videos.xml?playlist_id=PLAYLISTID

Here is how that is done:

    Open the playlist on YouTube, e.g., https://www.youtube.com/watch?v=ktxBUqy6PT4&list=PLYH8WvNV1YEnOwmzyWz4vR0HsX1Qn0PoU
    The ID begins after list=; in the case above, it is PLYH8WvNV1YEnOwmzyWz4vR0HsX1Qn0PoU
    Replace PLAYLISTID of the default feed address with the real ID to create the RSS feed for the playlist. In the example above, you get https://www.youtube.com/feeds/videos.xml?playlist_id=PLYH8WvNV1YEnOwmzyWz4vR0HsX1Qn0PoU



Create the YouTube channel RSS feed URL

Now that you have the channel ID and the default feed address, you can combine the two to create a working feed address:

    Default URL: https://www.youtube.com/feeds/videos.xml?channel_id=CHANNELID
    Channel ID: UCX6OQ3DkcsbYNE6H8uQQuVA
    Working Feed URL: https://www.youtube.com/feeds/videos.xml?channel_id=UCX6OQ3DkcsbYNE6H8uQQuVA



create youtube search result RSS feed

https://gist.github.com/JuniorJPDJ/aa26d4c61bc1e78af039e9a17bc17907




all the above is based on the youtube api,such as :
```
https://www.youtube.com/feeds/videos.xml?channel_id=...
```

but 
>In addition to the ?channel_id= parameter, there's also ?user= and ?playlist_id= with their values being the same as for /user/ and /playlist?list= URLs.

It's worth noting that only 15 videos are included in the feed, which may not be what the user expects. As with the previous case involving bare /channel/-URLs, a redirect to the equivalent non-feed URL - or at least a warning, may be in order.
 
