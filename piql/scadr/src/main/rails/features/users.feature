Feature: Users

  Scenario: Creating a User
    Given I am on the home page
    Then I should see "Create New User"
    When I follow "Create New User"
    Then I should be on the new user page
    
    When I fill in "Username" with "Karl"
    And I fill in "Home town" with "Berkeley"
    And I fill in "Password" with "asdf"
    And I fill in "Confirm password" with "asdf"
    And I press "Submit"
    Then I should be on the home page
    And I should see /Your account "Karl" has been created!/
    
    When I fill in "Username" with "Karl"
    And I fill in "Password" with "asdf"
    And I press "Login"
    Then I should be on Karl's user page