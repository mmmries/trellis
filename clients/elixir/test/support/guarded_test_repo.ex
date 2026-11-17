defmodule Trellis.GuardedTestRepo do
  @moduledoc false
  # A repo with the target guard, for test/trellis/target_test.exs.
  use Ecto.Repo, otp_app: :trellis_pg, adapter: Ecto.Adapters.Postgres
  use Trellis.Repo
end
