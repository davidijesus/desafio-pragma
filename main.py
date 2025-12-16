"""
Desafio de Estágio: Processamento de Logs do Quake III Arena

O que este script faz na prática:

- Lê o log bruto do Quake.
- Divide em partidas usando "InitGame:" como delimitador.
- Para cada partida, extrai:
  - game (número sequencial)
  - map (mapname do InitGame)
  - total_kills (quantidade de eventos Kill)
  - players (lista com id, nomes, kills, deaths, suicides, favorite_weapon, collected_items)
- Gera saída JSON no formato solicitado.
- Converte os mesmos dados para Parquet, preservando estrutura (listas/structs).
- Executa análises simples a partir dos dados consolidados.

Decisões tomadas para melhorar análises:

- Eu trato "<world>" (killer id 1022) como morte ambiental: incrementa deaths do jogador, mas não conta kill para ninguém.
- Suicídio é quando killer_id == victim_id (e não é 1022): incrementa suicides e deaths.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pyarrow as pa
import pyarrow.parquet as pq


WORLD_ID = 1022


@dataclass
class PlayerStats:
    player_id: int
    current_name: str = ""
    old_names: List[str] = field(default_factory=list)
    kills: int = 0
    deaths: int = 0
    suicides: int = 0
    weapon_kills: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    collected_items: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        # Arma favorita = arma com mais kills; se não houver, "none"
        favorite_weapon = "none"
        if self.weapon_kills:
            favorite_weapon = max(self.weapon_kills.items(), key=lambda kv: kv[1])[0]

        return {
            "id": self.player_id,
            "current_name": self.current_name,
            "old_names": self.old_names,
            "kills": self.kills,
            "deaths": self.deaths,
            "suicides": self.suicides,
            "favorite_weapon": favorite_weapon,
            "collected_items": self.collected_items,
        }


@dataclass
class GameStats:
    game_number: int
    map_name: str = ""
    total_kills: int = 0
    players_by_id: Dict[int, PlayerStats] = field(default_factory=dict)

    def ensure_player(self, player_id: int) -> PlayerStats:
        # Crio o jogador sob demanda porque o log pode trazer eventos fora de uma ordem "bonitinha"
        if player_id not in self.players_by_id:
            self.players_by_id[player_id] = PlayerStats(player_id=player_id)
        return self.players_by_id[player_id]

    def to_dict(self) -> Dict[str, Any]:
        # Exporto players como lista (ordem estável por id pra facilitar comparar output)
        players_list = [self.players_by_id[k].to_dict() for k in sorted(self.players_by_id)]
        return {
            "game": self.game_number,
            "map": self.map_name,
            "total_kills": self.total_kills,
            "players": players_list,
        }


def resolve_input_path(raw_name: str) -> Path:
    # Coloco a base do caminho
    base = Path("dados") / raw_name

    # Se o usuário já passou com extensão e existir, uso direto
    if base.exists() and base.is_file():
        return base

    # Tentativas comuns (porque às vezes o arquivo veio como .txt)
    candidates = [
        Path("dados") / f"{raw_name}.txt",
        Path("dados") / f"{raw_name}.log",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    raise FileNotFoundError(
        f"Não encontrei o arquivo em: {base} (nem variações .txt/.log). "
        f"Confere se está na pasta data/."
    )


def parse_map_from_initgame(line: str) -> str:
    m = re.search(r"\\mapname\\([^\\]+)", line)
    return m.group(1) if m else ""


def parse_userinfo_name(line: str) -> Tuple[int, str]:
    id_match = re.search(r"(?:^\d+:\d+\s+)?ClientUserinfoChanged:\s*(\d+)", line)
    if not id_match:
        raise ValueError(f"Não consegui extrair id de: {line.strip()}")

    player_id = int(id_match.group(1))

    name_match = re.search(r"\\n\\([^\\]+)", line)
    name = name_match.group(1) if name_match else ""
    return player_id, name



def parse_item_event(line: str) -> Tuple[int, str]:
    m = re.search(r"(?:^\d+:\d+\s+)?Item:\s*(\d+)\s+(.+)$", line.strip())
    if not m:
        raise ValueError(f"Linha de Item fora do padrão esperado: {line.strip()}")
    return int(m.group(1)), m.group(2).strip()



def parse_kill_event(line: str) -> Tuple[int, int, str]:
    ids_match = re.search(r"(?:^\d+:\d+\s+)?Kill:\s*(\d+)\s+(\d+)\s+\d+:", line)
    if not ids_match:
        raise ValueError(f"Não consegui parsear ids do Kill: {line.strip()}")

    killer_id = int(ids_match.group(1))
    victim_id = int(ids_match.group(2))

    mod_match = re.search(r"\sby\s+([A-Z0-9_]+)\s*$", line.strip())
    mod = mod_match.group(1) if mod_match else "UNKNOWN"

    return killer_id, victim_id, mod


def update_player_name(game: GameStats, line: str) -> None:
    #  Atualizo nome e mantenho histórico (old_names) se houver troca real
    player_id, new_name = parse_userinfo_name(line)
    player = game.ensure_player(player_id)

    # Guardar nomes antigos só quando já existe um nome válido
    if player.current_name and new_name and new_name != player.current_name:
        if player.current_name not in player.old_names:
            player.old_names.append(player.current_name)

    # Nome atual sempre fica com o último valor observado
    if new_name:
        player.current_name = new_name


def register_item_pickup(game: GameStats, line: str) -> None:
    # Itens coletados: guardo em lista sem duplicar, mantendo ordem
    player_id, item_name = parse_item_event(line)
    player = game.ensure_player(player_id)

    if item_name and item_name not in player.collected_items:
        player.collected_items.append(item_name)


def register_kill(game: GameStats, line: str) -> None:
    # Toda linha Kill incrementa total_kills da partida
    killer_id, victim_id, mod = parse_kill_event(line)
    game.total_kills += 1

    # Garante vítima (se não for world)
    if victim_id != WORLD_ID:
        victim = game.ensure_player(victim_id)
        victim.deaths += 1
    else:
        # Se por algum motivo a vítima vier como world (bem raro), só ignoro
        return

    # Kill ambiental: não atribuo kill a ninguém
    if killer_id == WORLD_ID:
        return

    # Suicídio: killer == victim (e não é world)
    if killer_id == victim_id:
        killer = game.ensure_player(killer_id)
        killer.suicides += 1
        return

    # Kill "normal": soma kill do killer e arma usada
    killer = game.ensure_player(killer_id)
    killer.kills += 1
    killer.weapon_kills[mod] += 1

def normalize_event_line(raw_line: str) -> str:
    # Tiro \n e espaços “sobrando”
    line = raw_line.rstrip("\n").strip()

    # Removo timestamp do começo, porque no log real Kill/Item quase sempre vêm com esse prefixo.
    line = re.sub(r"^\d+:\d+\s+", "", line)

    return line

def parse_log_into_games(lines: List[str]) -> List[GameStats]:
    games: List[GameStats] = []
    current_game: Optional[GameStats] = None
    game_counter = 0

    for raw_line in lines:
      original_line = raw_line.rstrip("\n")
      event_line = normalize_event_line(original_line)

      # InitGame pode vir com timestamp também, então checo no event_line
      if "InitGame:" in event_line:
          if current_game is not None:
              games.append(current_game)

          game_counter += 1
          current_game = GameStats(game_number=game_counter)
          current_game.map_name = parse_map_from_initgame(event_line)
          continue

      if current_game is None:
          continue

      if "ClientUserinfoChanged:" in event_line:
          update_player_name(current_game, event_line)
          continue

      if event_line.startswith("Item:"):
          register_item_pickup(current_game, event_line)
          continue

      if event_line.startswith("Kill:"):
          register_kill(current_game, event_line)
          continue


    # No fim do arquivo, não esqueço de salvar a última partida
    if current_game is not None:
        games.append(current_game)

    return games


def build_output_json(games: List[GameStats]) -> Dict[str, Any]:
    # Modelo de saída segue o enunciado: {"games": [ ... ]}
    return {"games": [g.to_dict() for g in games]}


def write_json(payload: Dict[str, Any], out_path: Path) -> None:
    # Salvo com indentação pra ficar legível e fácil de validar
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_parquet(payload: Dict[str, Any], out_path: Path) -> None:
    # Parquet com schema claro e estrutura preservada (listas/structs)
    # Uso pyarrow porque ele lida bem com nested (lista de struct)
    
        
    out_path.parent.mkdir(parents=True, exist_ok=True)

    games_list = payload.get("games", [])
    table = pa.Table.from_pylist(games_list)  # inferência de schema baseada no conteúdo

    # Escrevo um único arquivo parquet com todas as partidas
    pq.write_table(table, str(out_path))


def run_analyses(payload: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    # Três análises simples pra validar e extrair insight do dataset
    out_dir.mkdir(parents=True, exist_ok=True)

    games = payload.get("games", [])

    # A) Ranking global de kills por jogador (somando todas as partidas)
    kills_by_player = Counter()
    deaths_by_player = Counter()
    suicides_by_player = Counter()
    weapon_kills_global = Counter()
    kills_by_map = Counter()

    for g in games:
        kills_by_map[g.get("map", "")] += int(g.get("total_kills", 0))

        for p in g.get("players", []):
            name = p.get("current_name") or f"id_{p.get('id')}"
            kills_by_player[name] += int(p.get("kills", 0))
            deaths_by_player[name] += int(p.get("deaths", 0))
            suicides_by_player[name] += int(p.get("suicides", 0))

            fav = p.get("favorite_weapon", "none")
            # Aqui eu não uso fav como proxy de arma global, então conto só como “preferida”
            # Para kills por arma de verdade, eu teria que exportar weapon_kills também.
            # Mantive assim porque o enunciado pede favorite_weapon, não distribuição completa.
            if fav and fav != "none":
                weapon_kills_global[fav] += 1

    # A1. Top 10 jogadores por kills
    top_killers = kills_by_player.most_common(10)

    # B) Mapas com mais mortes totais (top 5)
    top_maps = kills_by_map.most_common(5)

    # C) Armas favoritas mais frequentes (top 5)
    top_favorite_weapons = weapon_kills_global.most_common(5)

    results = {
        "top_killers": top_killers,
        "top_maps_by_total_kills": top_maps,
        "most_frequent_favorite_weapons": top_favorite_weapons,
        "players_total_deaths": deaths_by_player.most_common(10),
        "players_total_suicides": suicides_by_player.most_common(10),
    }

    # Persisto um resumo em texto, fácil de anexar na entrega
    txt_path = out_dir / "analyses.txt"
    with txt_path.open("w", encoding="utf-8") as f:
        f.write("ANÁLISES DO DATASET (resumo)\n\n")

        f.write("Top jogadores por kills (global):\n")
        for name, val in top_killers:
            f.write(f"  - {name}: {val}\n")

        f.write("\nMapas com mais total_kills:\n")
        for name, val in top_maps:
            f.write(f"  - {name}: {val}\n")

        f.write("\nArmas favoritas mais frequentes:\n")
        for name, val in top_favorite_weapons:
            f.write(f"  - {name}: {val}\n")

    return results


def read_lines(input_path: Path) -> List[str]:
    # Leio em UTF-8 com fallback porque alguns logs podem ter caracteres estranhos
    try:
        return input_path.read_text(encoding="utf-8").splitlines(True)
    except UnicodeDecodeError:
        return input_path.read_text(encoding="latin-1").splitlines(True)


def print_results_summary(payload: Dict[str, Any], analyses: Dict[str, Any]) -> None:
    # Resumo final
    games_count = len(payload.get("games", []))
    print("\nRESULTADO FINAL")
    print(f"- Partidas processadas: {games_count}")

    if games_count > 0:
        first = payload["games"][0]
        print(f"- Exemplo (primeira partida): game={first.get('game')} map={first.get('map')} total_kills={first.get('total_kills')}")

    # Mostro só o essencial das análises pra não poluir terminal
    top_killers = analyses.get("top_killers", [])
    if top_killers:
        print(f"- Top killer global: {top_killers[0][0]} ({top_killers[0][1]} kills)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parser de log do Quake III Arena + análises")
    parser.add_argument("--file", default="Quake 1", help="Quake 1")
    parser.add_argument("--out", default="saida", help="saida")
    args = parser.parse_args()

    input_path = resolve_input_path(args.file)
    out_dir = Path(args.out)

    lines = read_lines(input_path)
    games = parse_log_into_games(lines)

    payload = build_output_json(games)

    json_path = out_dir / "games.json"
    parquet_path = out_dir / "games.parquet"

    write_json(payload, json_path)
    write_parquet(payload, parquet_path)

    analyses = run_analyses(payload, out_dir)
    print_results_summary(payload, analyses)

    print("\nArquivos gerados:")
    print(f"- {json_path}")
    print(f"- {parquet_path}")
    print(f"- {out_dir / 'analyses.txt'}")


if __name__ == "__main__":
    main()
