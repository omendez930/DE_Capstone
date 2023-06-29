def generate_ascii_card(number, name, expiration_date):
    card = [
        "  ________________________________________________  ",
        " |                                                | ",
        " |                                                | ",
        " |                                                | ",
        " |                                                | ",
        f" | {number}                            | ",
        " |                                                | ",
        " |                                                | ",
        f" | {name}                                       | ",
        " |                                                | ",
        f" |{expiration_date}                                           | ",
        " |________________________________________________| "
    ]

    return '\n'.join(card)

# Example usage
card_number = "**** **** **** 1234"  # Replace with actual card number
card_name = "John Doe"  # Replace with cardholder name
expiration_date = "06/25"  # Replace with expiration date

ascii_card = generate_ascii_card(card_number, card_name, expiration_date)
print(ascii_card)