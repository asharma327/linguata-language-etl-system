import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"      # or your EB URL
DATABASE = "japanese"      
DRY_RUN = False
MODEL = "gpt-4o-mini"

TITLES = [
    "unit1_grammar_do_the_japanese_understand_english",
    "unit1_grammar_forms_of_address",
    "unit1_grammar_greetings",
    "unit1_grammar_japanese_names",
    "unit1_grammar_notes_on_pronunciation_accent",
    "unit1_grammar_notes_on_pronunciation_sounds",
    "unit1_grammar_notes_on_pronunciation_the_syllable",
    "unit4_grammar_existence_of_an_object",
    "unit4_grammar_finding_english_speaker",
    "unit4_grammar_location_of_an_object",
    "unit4_grammar_please_write_it_here",
    "unit5_grammar_confirmation_particle_ne",
    "unit5_grammar_greeting_expressions_in_introductions",
    "unit5_grammar_kinship_terms",
    "unit5_grammar_kore_vs_kochira",
    "unit5_grammar_meeshi",
    "unit5_grammar_nihongo_ga_ojoozu_desu_ne",
    "unit5_grammar_particle_no",
    "unit2_grammar_how_to_say_thank_you",
    "unit2_grammar_ka_as_question_marker",
    "unit2_grammar_kokowa_desuka_questions",
    "unit2_grammar_mooichido_yukkuri_ittekudasai",
    "unit2_grammar_notes_on_the_writing_system",
    "unit2_grammar_particles",
    "unit2_grammar_sumimasen",
    "unit3_grammar_dont_understand_japanese",
    "unit3_grammar_finding_object_name",
    "unit3_grammar_how_to_indicate_acknowledgement",
    "unit3_grammar_how_to_say_ill_take_it",
    "unit3_grammar_particles_ga_and_de",
    "unit3_grammar_this_and_that",
    "unit10_grammar_gurai",
    "unit10_grammar_helpful_things",
    "unit10_grammar_made",
    "unit10_grammar_other_expressions",
    "unit10_grammar_tooi",
    "unit10_grammar_two_sets_of_numerals",
    "unit6_grammar_answers_about_location_of_place",
    "unit6_grammar_asking_location_of_place",
    "unit7_grammar_particle_no_and_place_expressions",
    "unit7_grammar_some_direction_words",
    "unit7_grammar_summary_of_basic_location_words",
    "unit8_grammar_helpful_things_to_know_in_getting_directions",
    "unit8_grammar_is_it_a_or_b",
    "unit8_grammar_some_important_words_in_getting_directions",
    "unit9_grammar_helpful_things_to_know_in_getting_directions",
    "unit9_grammar_more_key_words_in_comprehending_directions",
    "unit9_grammar_verbs_frequently_used_in_directions",
    "unit11_grammar_how_to_buy_correct_ticket",
    "unit11_grammar_how_to_express_desire",
    "unit11_grammar_kaisatsuguchi",
    "unit11_grammar_kinds_of_tickets",
    "unit11_grammar_public_transport",
    "unit11_grammar_tsurete_itte_agemashoo",
    "unit11_grammar_useful_expressions_for_buying_ticket",
    "unit12_grammar_densha",
    "unit12_grammar_doozou",
    "unit12_grammar_how_to_find_out_the_right_track_platform",
    "unit12_grammar_ikimasu_and_tomarimasu",
    "unit12_grammar_more_information_on_trains",
    "unit12_grammar_particle_to",
    "unit12_grammar_yokohama_iki",
    "unit13_grammar_basu",
    "unit13_grammar_ordinal_numbers",
    "unit13_grammar_questions_using_ordinal_numbers",
    "unit13_grammar_tsugi",
    "unit13_grammar_useful_expressions_getting_off",
    "unit14_grammar_de_after_expressions_of_location",
    "unit14_grammar_deguchi",
    "unit14_grammar_kaidan",
    "unit14_grammar_norikae",
    "unit15_grammar_how_to_pay_fare",
    "unit15_grammar_how_to_stop_cab",
    "unit15_grammar_how_to_tell_destination",
    "unit15_grammar_ookii",
    "unit16_grammar_how_to_locate_merchandise",
    "unit17_grammar_conversion_tables",
    "unit17_grammar_indicate_preference_different",
    "unit17_grammar_indicate_preference_larger",
    "unit17_grammar_kono_sono_ano",
    "unit17_grammar_location_of_merchandise",
    "unit17_grammar_polite_expressions_salesclerks",
    "unit17_grammar_when_you_want_to_say_please_show_me",
    "unit18_grammar_500_yen_per_100_grams",
    "unit18_grammar_bargaining",
    "unit18_grammar_decide_not_to_buy",
    "unit18_grammar_kore_o_kudasai",
    "unit19_grammar_counters_for_fruits_and_vegetables",
    "unit19_grammar_different_way_of_giving_change",
    "unit19_grammar_how_to_ask_total_price",
    "unit19_grammar_otsuri_change",
    "unit19_grammar_when_you_leave_the_store",
    "unit20_grammar_four_seasons_and_climate",
    "unit20_grammar_how_to_close_an_encounter",
    "unit20_grammar_how_to_describe_temperature",
    "unit20_grammar_set_expressions_for_greetings",
    "unit20_grammar_weather_terminology_and_greetings",
    "unit20_grammar_where_are_you_going",
    "unit21_grammar_how_to_find_the_dish_you_want",
    "unit21_grammar_seating",
    "unit21_grammar_shokken_meal_ticket",
    "unit22_grammar_how_to_indicate_ready_to_order",
    "unit22_grammar_useful_expressions_for_ordering",
    "unit23_grammar_finding_out_about_the_taste_of_a_dish",
    "unit23_grammar_hai_as_no_and_iie_as_yes",
    "unit23_grammar_paying_the_check_and_leaving_the_restaurant",
    "unit23_grammar_which_dish_do_you_recommend",
    "unit24_grammar_how_to_answer_a_call",
    "unit24_grammar_how_to_get_the_person_you_want_to_talk_to",
    "unit24_grammar_how_to_use_public_telephones",
    "unit25_grammar_how_to_answer_a_call_in_your_office",
    "unit25_grammar_how_to_call_a_person_at_his_her_office",
    "unit26_grammar_how_to_say_ill_call_again",
    "unit26_grammar_how_to_say_name_isnt_in_now",
    "unit26_grammar_is_there_someone_who_understands_english",
    "unit27_grammar_some_basic_expressions_for_describing_symptoms",
    "unit27_grammar_what_to_do_when_you_are_involved_in_a_car_accident",
    "unit28_grammar_about_electrical_equipment",
    "unit28_grammar_when_things_break",
    "unit29_grammar_buying_tickets",
    "unit29_grammar_deciding_the_place_and_time_to_meet",
    "unit29_grammar_deciding_the_time",
    "unit29_grammar_how_to_ask_someone_to_go_out_to_dinner_or_for_drinks",
    "unit29_grammar_shall_we_go_and_see",
    "unit30_grammar_other_sentiments",
    "unit30_grammar_tanoshikatta_desu",
    "unit30_grammar_when_you_have_a_japanese_guest",
    "unit30_grammar_when_you_visit_a_japanese_home",
    "unit30_grammar_when_you_want_to_take_leave"
]

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]


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
        "openai_api_key": OPENAI_API_KEY,     # sent in the request body, like the other routes
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