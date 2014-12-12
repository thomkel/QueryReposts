require 'cassandra'

class RepostsController < ApplicationController

  def index
    render 'welcome'
  end

  # GET /reposts
  # GET /reposts.json
  def votes_by_times_reposted
    connect_to_cluster

    @votes_by_group = []
    @latest_posts = []
    @data_by_counts = Hash.new([0,0,0])
    @counts = []
    @posts = Hash.new(-1)
    @image_ids = []

    get_realtime_post_data
    get_historical_post_data
    # get historial post data

    @session.close

  end

  def sum_data_by_count_info(count, t_votes, u_votes, d_votes)
    count_info = @data_by_counts[count]

    votes = count_info[0] + t_votes
    upvotes = count_info[1] + u_votes
    downvotes = count_info[2] + d_votes

    @data_by_counts[count] = [votes, upvotes, downvotes]
    @counts[count] = true
  end

  def get_historical_post_data
    @not_found = []

    @session.execute("SELECT * FROM votes_by_group_id").each do |row|
      image_id = row['image_id']
      count = row['count']
      total_votes = row['votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']


      # if no realtime post with image id
      if @posts[image_id] != -1  
        @found = "true"
        post_info = @posts[image_id]
        total_votes = total_votes + post_info[0]
        upvotes = upvotes + post_info[1]
        downvotes = downvotes + post_info[2]
        count = count + post_info[3]

        votes_by_group_info = [image_id, count, total_votes,
          upvotes, downvotes]

        @votes_by_group.push(votes_by_group_info)

        sum_data_by_count_info(count, total_votes, upvotes, downvotes)

        @posts[image_id] = -1
      else
        @not_found.push(image_id)

        @votes_by_group.push([image_id, count, total_votes,
          upvotes, downvotes
        ])

        sum_data_by_count_info(count, total_votes, upvotes, downvotes)
      end

      # if no historical data
      @image_ids.each do |post|
        if (@posts[post] != -1) & !post.nil?
          post_info = @posts[post]

          [total_votes, upvotes, downvotes, 1]

          @votes_by_group.push([post, post_info[3], post_info[0],
            post_info[1], post_info[2]
          ])

          sum_data_by_count_info(post_info[3], post_info[0],
            post_info[1], post_info[2]) 

          @posts[post] = -1           
        end
      end
    end
  end

  def get_realtime_post_data
    @session.execute("SELECT * FROM latest_posts").each do |row|
      image_id = row['image_id']
      total_votes = row['total_votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      if !image_id.nil?

        # put post info in hash table
        if @posts[image_id] == -1
          @posts[image_id] = [total_votes, upvotes, downvotes, 1]

          @image_ids.push(image_id)  
        # if already in hash table, sum post info    
        else
          post_info = @posts[image_id]

          total_votes = total_votes + post_info[0]
          upvotes = upvotes + post_info[1]
          downvotes = downvotes + post_info[2]
          count = post_info[3] + 1

          @posts[image_id] = [total_votes, upvotes, downvotes, count]
        end
      end
    end  

    @test_posts = @posts.clone
  end   

  def votes_by_subreddit
    connect_to_cluster

    @votes_by_group = []
    @latest_posts = []
    @data_by_counts = Hash.new([0,0,0,0])
    @counts = []
    @posts = Hash.new(-1)
    @subreddits = []

    get_realtime_subreddit_data
    get_historical_subreddit_data
    # get_realtime_post_data
    # get_historical_post_data
    # # get historial post data

    @session.close
  end

  def get_realtime_subreddit_data
    @session.execute("SELECT * FROM latest_posts").each do |row|
      subreddit = row['subreddit']
      total_votes = row['total_votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      # put post info in hash table
      if @posts[subreddit.to_s] == -1
        @posts[subreddit.to_s] = [total_votes, upvotes, downvotes, 1]

        @subreddits.push(subreddit)  
      # if already in hash table, sum post info    
      else
        post_info = @posts[subreddit.to_s]

        total_votes = total_votes + post_info[0]
        upvotes = upvotes + post_info[1]
        downvotes = downvotes + post_info[2]
        count = post_info[3] + 1

        @posts[subreddit.to_s] = [total_votes, upvotes, downvotes, count]
      end
    end  

    @test_posts = @posts.clone
  end

  def sum_data_by_subreddit(subreddit, count, t_votes, u_votes, d_votes)
    count_info = @data_by_counts[subreddit.to_s]

    count = count_info[0] + count
    votes = count_info[1] + t_votes
    upvotes = count_info[2] + u_votes
    downvotes = count_info[3] + d_votes

    @data_by_counts[subreddit.to_s] = [count, votes, upvotes, downvotes]
    # @counts[subreddit] = true
  end

  def get_historical_subreddit_data
    @not_found = []
    @found = []

    @session.execute("SELECT * FROM votes_by_subreddit").each do |row|
      subreddit = row['subreddit']
      count = row['count']
      total_votes = row['votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      # if no realtime post with image id
      if @posts[subreddit.to_s] != -1  
        @found.push(subreddit)
        post_info = @posts[subreddit.to_s]
        total_votes = total_votes + post_info[0]
        upvotes = upvotes + post_info[1]
        downvotes = downvotes + post_info[2]
        count = count + post_info[3]

        votes_by_group_info = [subreddit, count, total_votes,
          upvotes, downvotes]

        @votes_by_group.push(votes_by_group_info)

        sum_data_by_subreddit(subreddit.to_s, count, total_votes, upvotes, downvotes)

        @posts[subreddit.to_s] = -1
      else
        @votes_by_group.push([row['subreddit'], row['count'], row['votes'],
          row['upvotes'], row['downvotes']
        ])

        @not_found.push(subreddit)
        sum_data_by_subreddit(subreddit, count, total_votes, upvotes, downvotes)
      end

      # if no historical data
      @subreddits.each do |post|
        if @posts[post] != -1
          post_info = @posts[post]

          [total_votes, upvotes, downvotes, 1]

          @votes_by_group.push([post, post_info[3], post_info[0],
            post_info[1], post_info[2]
          ])

          sum_data_by_subreddit(post, post_info[3], post_info[0],
            post_info[1], post_info[2]) 

          @posts[post] = -1           
        end
      end
    end
  end

  def votes_by_repost_num
    connect_to_cluster

    @realtime_reposts = Hash.new(-1)
    @realtime_image_ids = []

    @session.execute("SELECT * FROM latest_posts").each do |row|
      image_id = row['image_id']
      total_votes = row['total_votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']    
      unixtime = row['unixtime']

      if @realtime_reposts[image_id] == -1
        @realtime_image_ids.push(image_id)  
        @realtime_reposts[image_id] = [[unixtime, total_votes, upvotes, downvotes]] 
      else
        @realtime_reposts[image_id] = consolidate_realtime_votes(@realtime_reposts[image_id], unixtime, total_votes, upvotes, downvotes)
      end
    end

    @realtime_image_ids.each do |image_id|
      @raw_posts = get_vote_data_for_image_id(image_id)

      if @raw_posts.size > 0 
        @rt_posts_test = @realtime_reposts[image_id]

        @realtime_reposts[image_id] = consolidate_realtime_and_raw_votes(@raw_posts, @realtime_reposts[image_id])
      end
    end

    @reposts_hash = consolidate_historical_and_realtime_votes(get_posts_by_reposts_num, @realtime_reposts, @realtime_image_ids)  
    @repost_nums = transfer_repost_data_to_array(@reposts_hash)

    @session.close
  end

  def consolidate_realtime_and_raw_votes(raw_posts, realtime_posts)
    # raw data= [[raw_unixtime, raw_votes, raw_upvotes, raw_downvotes]]
    # realtime = [[unixtime, total_votes, upvotes, downvotes]] 
    @copy_raw_posts = Array.new(raw_posts)

    size = realtime_posts.size

    for i in 0..size-1
      @copy_raw_posts[raw_posts.size + i] = [Float::INFINITY, 0, 0, 0]
    end

    @index = 0

    realtime_posts.each do |rt_post|
      while (@index < @copy_raw_posts.size-1)
        if (@copy_raw_posts[@index][0] < rt_post[0])
          @index = @index + 1
        else
          break
        end
      end

      temp = [rt_post[0], rt_post[1], rt_post[2], rt_post[3]]

      for i in @index..@copy_raw_posts.size-1
        @p_index = @copy_raw_posts[i]

        new_array = [temp[0], temp[1]-@p_index[1], temp[2]-@p_index[2], temp[3]-@p_index[3]]
        @copy_raw_posts[i] = new_array

        temp = @p_index
      end
    end

    return @copy_raw_posts
  end  

  def consolidate_realtime_votes(posts, unixtime, total_votes, upvotes, downvotes)
    index = 0

    while (posts[index][0] < unixtime) & (index < posts.size-1)
      index = index + 1
    end
    temp = [unixtime, total_votes, upvotes, downvotes]

    posts.insert(index, temp)

    return posts
  end

  def transfer_repost_data_to_array(reposts_hash)
    count = 1
    reposts = []

    while true
      repost_data = reposts_hash[count]
      if repost_data == -1
        break
      end
      repost_data.unshift(count)
      count = count + 1

      reposts.push(repost_data)
    end

    return reposts
  end

  def consolidate_historical_and_realtime_votes(h_posts, rt_posts, image_ids)

    image_ids.each do |image_id|
      count = 0
      rt_data = rt_posts[image_id]

      while count < rt_data.size
        hist_data = h_posts[count+1]

        if hist_data == -1
          h_posts[count+1] = [rt_data[count][1], rt_data[count][2], rt_data[count][3]]
        else
          hist_data[0] = hist_data[0] + rt_data[count][1]
          hist_data[1] = hist_data[1] + rt_data[count][2]
          hist_data[2] = hist_data[2] + rt_data[count][3]
          h_posts[count+1] = hist_data
        end

        count = count + 1
      end 
    end

    return h_posts
  end 

  def get_vote_data_for_image_id(image_id)
    posts = []
    image = image_id.to_i

    @session.execute("SELECT * FROM votes_by_repost_num_raw WHERE image_id = #{image} ALLOW FILTERING").each do |row|

      raw_count = row['count']
      raw_votes = row['votes']
      raw_upvotes = row['upvotes']
      raw_downvotes = row['downvotes']
      raw_unixtime = row['unixtime']

      posts[raw_count-1] = [raw_unixtime, raw_votes, raw_upvotes, raw_downvotes]
    end

    return posts
  end

  def get_posts_by_reposts_num
    repost_nums = Hash.new(-1)

    @session.execute("SELECT * FROM votes_by_repost_num").each do |row|
      count = row['count']
      total_votes = row['votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      repost_nums[count] = [total_votes, upvotes, downvotes]
    end    

    @test_h = repost_nums.clone
    return repost_nums    
  end

  def get_votes_stats_by_subreddit

  end

  def connect_to_cluster
    cluster = Cassandra.cluster
    keyspace = 'reddit_posts'
    @session = cluster.connect(keyspace)
  end
end
