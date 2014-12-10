require 'cassandra'

class RepostsController < ApplicationController
  # before_action :set_repost, only: [:show, :edit, :update, :destroy]

  # GET /reposts
  # GET /reposts.json
  def index
    connect_to_cluster

    @votes_by_group = []
    @latest_posts = []
    @data_by_counts = Hash.new([0,0,0])
    @counts = []

    posts = Hash.new(-1)
    image_ids = []

    # get realtime post data
    @session.execute("SELECT * FROM latest_posts").each do |row|
      image_id = row['image_id']
      total_votes = row['total_votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      # put post info in hash table
      if posts[image_id] == -1
        posts[image_id] = [total_votes, upvotes, downvotes, 1]

        image_ids.push(image_id)  


      # if already in hash table, sum post info    
      else
        post_info = posts[image_id]

        total_votes = total_votes + post_info[0]
        upvotes = upvotes + post_info[1]
        downvotes = downvotes + post_info[2]
        count = post_info[3] + 1

        posts[image_id] = [total_votes, upvotes, downvotes, count]
      end
    end    

    # get historial post data
    @session.execute("SELECT * FROM votes_by_group_id").each do |row|
      image_id = row['image_id']
      count = row['count']
      total_votes = row['votes']
      upvotes = row['upvotes']
      downvotes = row['downvotes']

      # if no realtime post with image id
      if posts[image_id] == -1
        @votes_by_group.push([row['image_id'], row['count'], row['votes'],
          row['upvotes'], row['downvotes']
        ])

        sum_data_by_count_info(count, total_votes, upvotes, downvotes)


      # otherwise, sum historical/realtime info for image id
      else
        post_info = posts[image_id]
        total_votes = total_votes + post_info[0]
        upvotes = upvotes + post_info[1]
        downvotes = downvotes + post_info[2]
        count = count + post_info[3]

        votes_by_group_info = [image_id, count, total_votes,
          upvotes, downvotes]

        @votes_by_group.push(votes_by_group_info)

        sum_data_by_count_info(count, total_votes, upvotes, downvotes)

        posts[image_id] = -1
      end
      # for each image_id in real time data, add to votes_by_group
      # if no historical data
      image_ids.each do |post|
        if posts[post] != -1
          post_info = posts[post]

          [total_votes, upvotes, downvotes, 1]

          @votes_by_group.push([post, post_info[3], post_info[0],
            post_info[1], post_info[2]
          ])

          sum_data_by_count_info(post_info[3], post_info[0],
            post_info[1], post_info[2]) 

          posts[post] = -1           
        end
      end
    end

    @session.close
  end

  def sum_data_by_count_info(count, t_votes, u_votes, d_votes)
    count_info = @data_by_counts[count]

    votes = count_info[0] + t_votes
    upvotes = count_info[1] + u_votes
    downvotes = count_info[2] + d_votes

    @data_by_counts[count.to_i] = [votes, upvotes, downvotes]
    @counts[count] = true

  end

  def connect_to_cluster
    cluster = Cassandra.cluster
    keyspace = 'reddit_posts'
    @session = cluster.connect(keyspace)
  end

  # private
  #   # Use callbacks to share common setup or constraints between actions.
  #   def set_repost
  #     @repost = Repost.find(params[:id])
  #   end

  #   # Never trust parameters from the scary internet, only allow the white list through.
  #   def repost_params
  #     params[:repost]
  #   end
end
