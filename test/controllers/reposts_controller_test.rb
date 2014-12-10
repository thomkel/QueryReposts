require 'test_helper'

class RepostsControllerTest < ActionController::TestCase
  setup do
    @repost = reposts(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:reposts)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create repost" do
    assert_difference('Repost.count') do
      post :create, repost: {  }
    end

    assert_redirected_to repost_path(assigns(:repost))
  end

  test "should show repost" do
    get :show, id: @repost
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @repost
    assert_response :success
  end

  test "should update repost" do
    patch :update, id: @repost, repost: {  }
    assert_redirected_to repost_path(assigns(:repost))
  end

  test "should destroy repost" do
    assert_difference('Repost.count', -1) do
      delete :destroy, id: @repost
    end

    assert_redirected_to reposts_path
  end
end
