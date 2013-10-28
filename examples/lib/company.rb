class Company < ActiveRecord::Base
  extend BulkMethodsMixin
  has_many :employees, -> { where "companies.id = employees.companies_id" } :class_name => 'Company'

  connection.execute <<-SQL
    create table companies
    (
        id               serial not null primary key,
        created_at       timestamp not null default now(),
        updated_at       timestamp,
        name             text null
    );
  SQL
end

COMPANIES = [
             {
               :name => 'Fluent Mobile, inc.'
             },
             {
               :name => 'Fiksu, inc.'
             },
             {
               :name => 'AppExchanger, inc.'
             },
             {
               :name => 'FreeMyApps, inc.'
             },
]
