CREATE INDEX IF NOT EXISTS balance_changes_rune_id_address_block_height_index ON balance_changes (rune_id, address, block_height DESC);
