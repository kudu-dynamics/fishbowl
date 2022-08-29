defimpl Msgpax.Packer, for: Date do
  @date_ext_type 10

  def pack(date) do
    @date_ext_type
    |> Msgpax.Ext.new(Date.to_iso8601(date))
    |> Msgpax.Packer.pack()
  end
end
