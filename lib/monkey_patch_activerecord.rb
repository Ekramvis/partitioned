require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/persistence.rb'
require 'active_record/relation/query_methods.rb'

#
# Patching {ActiveRecord} to allow specifying the table name as a function of
# attributes.
#
module ActiveRecord
  #
  # Patches for Persistence to allow certain partitioning (that related to the primary key) to work.
  #
  module Persistence
    # Deletes the record in the database and freezes this instance to reflect
    # that no changes should be made (since they can't be persisted).
    def destroy
      destroy_associations

      if persisted?
        IdentityMap.remove(self) if IdentityMap.enabled?
        pk         = self.class.primary_key
        column     = self.class.columns_hash[pk]
        substitute = connection.substitute_at(column, 0)

        if self.class.respond_to?(:dynamic_arel_table)
          using_arel_table = dynamic_arel_table()
          relation = ActiveRecord::Relation.new(self.class, using_arel_table).
            where(using_arel_table[pk].eq(substitute))
        else
          using_arel_table = self.class.arel_table
          relation = self.class.unscoped.where(using_arel_table[pk].eq(substitute))
        end

        relation.bind_values = [[column, id]]
        relation.delete_all
      end

      @destroyed = true
      freeze
    end

    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def update_record(attribute_names = @attributes.keys)
      attributes_with_values = arel_attributes_with_values_for_update(attribute_names)
      if attributes_with_values.empty?
        0
      else
        klass = self.class
        column_hash = klass.connection.schema_cache.columns_hash klass.table_name
        db_columns_with_values = attributes_with_values.map { |attr,value|
          real_column = column_hash[attr.name]
          [real_column, value]
        }
        bind_attrs = attributes_with_values.dup
        bind_attrs.keys.each_with_index do |column, i|
          real_column = db_columns_with_values[i].first
          bind_attrs[column] = klass.connection.substitute_at(real_column, i)
        end
        stmt = klass.unscoped.where(klass.arel_table[klass.primary_key].eq(id_was || id)).arel.compile_update(bind_attrs)
        # use the partitioned table instead of the main table
        stmt.table(self.class.from_partition(*self.class.partition_key_values(@attributes)).table)
        klass.connection.update stmt, 'SQL', db_columns_with_values
      end
    end

    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    # Patch the create_record method to prefetch the primary key if needed
    def create_record(attribute_names = @attributes.keys)
      if self.id.nil? && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
        self.id = self.class.connection.next_sequence_value(self.class.sequence_name)
      end

      attributes_values = arel_attributes_with_values_for_create(attribute_names)

      new_id = self.class.from_partition(*self.class.partition_key_values(@attributes)).insert attributes_values
      self.id ||= new_id if self.class.primary_key

      @new_record = false
      id
    end
  end

  #
  # A patch to QueryMethods to change default behavior of select
  # to use the Relation's Arel::Table.
  #
  module QueryMethods

    def build_select(arel, selects)
      unless selects.empty?
        @implicit_readonly = false
        arel.project(*selects)
      else
        arel.project(table[Arel.star])
      end
    end

  end

  #
  # Patches for relation to allow back hooks into the {ActiveRecord}
  # requesting name of table as a function of attributes.
  #
  class Relation
    #
    # Patches {ActiveRecord}'s building of an insert statement to request
    # of the model a table name with respect to attribute values being
    # inserted.
    #
    # The differences between this and the original code are small and marked
    # with PARTITIONED comment.
    def insert(values)
      primary_key_value = nil

      if primary_key && Hash === values
        primary_key_value = values[values.keys.find { |k|
          k.name == primary_key
        }]

        if !primary_key_value && connection.prefetch_primary_key?(klass.table_name)
          primary_key_value = connection.next_sequence_value(klass.sequence_name)
          values[klass.arel_table[klass.primary_key]] = primary_key_value
        end
      end

      im = arel.create_insert
      #
      # PARTITIONED ADDITION. get arel_table from class with respect to the
      # current values to placed in the table (which hopefully hold the values
      # that are used to determine the child table this insert should be
      # redirected to)
      #
      actual_arel_table = @klass.dynamic_arel_table(Hash[*values.map{|k,v| [k.name,v]}.flatten]) if @klass.respond_to?(:dynamic_arel_table)
      actual_arel_table = @table unless actual_arel_table
      im.into actual_arel_table

      conn = @klass.connection

      substitutes = values.sort_by { |arel_attr,_| arel_attr.name }
      binds       = substitutes.map do |arel_attr, value|
        [@klass.columns_hash[arel_attr.name], value]
      end

      substitutes.each_with_index do |tuple, i|
        tuple[1] = conn.substitute_at(binds[i][0], i)
      end

      if values.empty? # empty insert
        im.values = Arel.sql(connection.empty_insert_statement_value)
      else
        im.insert substitutes
      end

      conn.insert(
        im,
        'SQL',
        primary_key,
        primary_key_value,
        nil,
        binds)
    end
  end
end
