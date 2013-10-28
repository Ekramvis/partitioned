#
# These are things our base class must fix in ActiveRecord::Base
#
# No need to monkey patch these, just override them.
#
module Partitioned
  #
  # methods that need to be override in an ActiveRecord::Base derived class so that we can support partitioning
  #
  module ActiveRecordOverrides
    #
    # Delete just needs a wrapper around it to specify the specific partition.
    #
    # @return [optional] undefined
    def delete
      if persisted?
        self.class.from_partition(*self.class.partition_key_values(attributes)).delete(id)
      end
      @destroyed = true
      freeze
    end
  end
end
