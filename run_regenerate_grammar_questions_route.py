import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"
DATABASE = "hindi"
DRY_RUN = True
MODEL = "gpt-4o-mini"

TITLES = [
    "unit16_population_and_cliches",
    # "unit17_present_tense_living",
    # "unit18_pronouns_and_gender",
    # "unit19_family_nouns",
    # "unit20_comparatives_and_oblique_pronouns",
    # "unit21_plurals_grammar",
    # "unit22_oblique_case_grammar",
    # "unit23_daily_routine_grammar",
    # "unit24_seasons_grammar",
    # "unit25_duration_and_commands_grammar",
    # "unit26_verb_chaahna_and_interrogatives",
    # "unit27_past_tense_apna_relative",
    # "unit28_past_habitual_and_relative",
    # "unit29_milna_availability_grammar",
    # "unit30_chahiye_grammar",
    # "unit10_origin_grammar",
    # "unit11_question_marker",
    # "unit12_adjectives",
    # "unit13_comparatives_and_superlatives",
    # "unit14_feminine_adjectives",
    # "unit15_numbers_and_comparatives",
    # "unit1_sentence_pattern",
    # "unit2_country_name_pattern",
    # "unit36_sakta_ability_grammar",
    # "unit37_past_ability_grammar",
    # "unit38_past_perfect_grammar",
    # "unit39_karna_construction_grammar",
    # "unit3_sentence_structure",
    # "unit40_past_expectation_grammar",
    # "unit41_subjunctive_grammar",
    # "unit42_ne_postposition_grammar",
    # "unit43_present_perfect_grammar",
    # "unit44_causative_verbs_grammar",
    # "unit45_obligation_and_clarifier_grammar",
    # "unit46_perfective_chukna_clarifier_grammar",
    # "unit47_presumptive_grammar",
    # "unit48_conditional_grammar",
    # "unit49_neuter_passive_and_causative_grammar",
    # "unit4_possessives_and_commands",
    # "unit50_passive_voice_grammar",
    # "unit51_participles_grammar",
    # "unit5_negatives_and_prohibitions",
    # "unit6_postpositions",
    # "unit7_days_dates_grammar",
    # "unit8_possessive_pronouns",
    # "unit9_noun_genders",
    # "unit31_past_obligation_grammar",
    # "unit32_present_continuous_grammar",
    # "unit33_indirect_object_grammar",
    # "unit34_future_tense_permission_grammar",
    # "unit35_numbers_time_grammar"
]


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "titles": TITLES,
        "model": MODEL,
        "dry_run": DRY_RUN,
    }

    with requests.post(f"{BASE}/regenerate-grammar-questions",
                       json=payload, stream=True, timeout=3600) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START regenerate-grammar-questions | model={ev['model']} | dry_run={ev['dry_run']}")
                print(f"  titles={ev['titles']}")
            elif e == "lesson_start":
                print(f"\n== {ev['title']} (lesson {ev['lesson_id']}) | "
                      f"{ev['learning']} learning, {ev['practice']} practice")
            elif e == "learning_rewrite":
                print(f"   L q{ev['question_id']}: {ev['new_question']!r}")
            elif e == "practice_rewrite":
                print(f"   P q{ev['question_id']}: {ev['new_question']!r}")
                print(f"       answer: {ev['new_answer']!r}")
            elif e == "shortfall":
                print(f"   ! SHORTFALL: wanted {ev['wanted_learning']}L/{ev['wanted_practice']}P, "
                      f"got {ev['got_learning']}L/{ev['got_practice']}P")
            elif e == "lesson_skipped":
                print(f"  LESSON SKIPPED {ev['title']}: {ev['reason']}")
            elif e == "lesson_error":
                print(f"  LESSON ERROR {ev['title']}: {ev['error']}")
            elif e == "lesson_done":
                print(f"   = {ev['title']}: {ev['learning_rewritten']}L + "
                      f"{ev['practice_rewritten']}P rewritten"
                      + (f" ({ev['note']})" if ev.get('note') else ""))
            elif e == "backfill":
                print(f"  backfill re-run on {ev['lessons']} lessons")
            elif e == "summary":
                print("\n" + "=" * 55)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()