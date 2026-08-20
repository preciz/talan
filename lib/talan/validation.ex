defmodule Talan.Validation do
  @moduledoc false

  @type unary_function :: (term() -> term())

  @spec positive_integer!(term(), atom()) :: pos_integer()
  def positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  def positive_integer!(value, name) do
    raise ArgumentError, "#{name} must be a positive integer, got: #{inspect(value)}"
  end

  @spec options!(term(), list(atom())) :: keyword()
  def options!(options, allowed_keys) do
    unless Keyword.keyword?(options) do
      raise ArgumentError, "options must be a keyword list, got: #{inspect(options)}"
    end

    case Keyword.keys(options) -- allowed_keys do
      [] ->
        options

      unknown_keys ->
        raise ArgumentError, "unknown options: #{inspect(Enum.uniq(unknown_keys))}"
    end
  end

  @spec probability!(term(), atom()) :: float()
  def probability!(value, _name) when is_number(value) and value > 0 and value < 1, do: value

  def probability!(value, name) do
    raise ArgumentError, "#{name} must be a number between 0 and 1, got: #{inspect(value)}"
  end

  @spec functions!(term(), atom()) :: list(unary_function())
  def functions!(functions, name) when is_list(functions) do
    if Enum.all?(functions, &is_function(&1, 1)) do
      functions
    else
      raise ArgumentError, "#{name} must contain only one-argument functions"
    end
  end

  def functions!(value, name) do
    raise ArgumentError,
          "#{name} must be a list of one-argument functions, got: #{inspect(value)}"
  end

  @spec function!(term(), atom()) :: unary_function()
  def function!(function, _name) when is_function(function, 1), do: function

  def function!(value, name) do
    raise ArgumentError, "#{name} must be a one-argument function, got: #{inspect(value)}"
  end

  @spec one_of!(value, list(value), atom()) :: value when value: term()
  def one_of!(value, allowed, name) do
    if value in allowed do
      value
    else
      raise ArgumentError, "#{name} must be one of #{inspect(allowed)}, got: #{inspect(value)}"
    end
  end

  @spec boolean!(term(), atom()) :: boolean()
  def boolean!(value, _name) when is_boolean(value), do: value

  def boolean!(value, name) do
    raise ArgumentError, "#{name} must be a boolean, got: #{inspect(value)}"
  end
end
